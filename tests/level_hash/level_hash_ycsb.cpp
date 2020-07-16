#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <iterator>
#include <thread>
#include <vector>
#include <sstream>
#include <cstdio>
#include <cassert>
#include <time.h>

#define LAYOUT "level_hash"
#define KEY_LEN 15

#include "../../examples/libpmemobj_cpp_examples_common.hpp"
#include "../polymorphic_string.h"
#include "../profile.hpp"
#include <libpmemobj++/experimental/level_hash.hpp>

// #define VALUE_LEN 16
// #define LATENCY_ENABLE 1

#ifdef MACRO_TEST_FOR_LEVEL_HASH

// (2^15 + 2^14) * 4 = 196608
#define HASH_POWER 15
#define READ_WRITE_NUM 64000000

#else

// (2^13 + 2^12) * 4 = 49152
#define HASH_POWER 13
#define READ_WRITE_NUM 16000000

#endif


namespace nvobj = pmem::obj;

namespace
{

class key_equal {
public:
	template <typename M, typename U>
	bool operator()(const M &lhs, const U &rhs) const
	{
		return lhs == rhs;
	}
};

class string_hasher {
	/* hash multiplier used by fibonacci hashing */
	static const size_t hash_multiplier = 11400714819323198485ULL;

public:
	using transparent_key_equal = key_equal;

	size_t operator()(const polymorphic_string &str) const
	{
		return hash(str.c_str(), str.size());
	}

private:
	size_t hash(const char *str, size_t size) const
	{
		size_t h = 0;
		for (size_t i = 0; i < size; ++i) {
			h = static_cast<size_t>(str[i]) ^ (h * hash_multiplier);
		}
		return h;
	}
};

using string_t = polymorphic_string;
typedef nvobj::experimental::level_hash<string_t, string_t, string_hasher,
	std::equal_to<string_t>>
	persistent_map_type;

struct root {
	nvobj::persistent_ptr<persistent_map_type> cons;
};

enum class level_hash_op {
	UNKNOWN,
	INSERT,
	READ,
	DELETE,
	UPDATE,

	MAX_OP
};

struct thread_queue {
	string_t key;
	level_hash_op operation;
};

struct sub_thread {
	uint32_t id;
	uint64_t inserted;
	uint64_t ins_failure;
	uint64_t found;
	uint64_t unfound;
	uint64_t deleted;
	uint64_t del_existing;
	uint64_t updated;
	uint64_t upd_existing;
	uint64_t thread_num;
	thread_queue *run_queue;
	double *latency_queue;
};

} /* Annoymous namespace */

int
main(int argc, char *argv[])
{
	char *ptr = getenv("PMEM_WRITE_LATENCY_IN_NS");
	if (ptr)
		printf("PMEM_WRITE_LATENCY_IN_NS set to %s (ns)\n", ptr);
	else
		printf("write latency is not set\n");

#ifdef LATENCY_ENABLE
	printf("LATENCY_ENABLE set\n");
#endif

	// parse inputs
	if (argc != 5) {
		printf("usage: %s <pool_path> <load_file> <run_file> <thread_num>\n\n", argv[0]);
		printf("    pool_path: the pool file required for PMDK\n");
		printf("    load_file: a workload file for the load phase\n");
		printf("    run_file: a workload file for the run phase\n");
		printf("    thread_num: the number of threads\n");
		exit(1);
	}

	printf("MACRO HASH_POWER: %d\n", HASH_POWER);

	const char *path = argv[1];
	size_t thread_num;

	std::stringstream s;
	s << argv[4];
	s >> thread_num;

	// initialize level_hash
	nvobj::pool<root> pop;
	remove(path); // delete the mapped file.

	pop = nvobj::pool<root>::create(
		path, LAYOUT, PMEMOBJ_MIN_POOL * 20480, S_IWUSR | S_IRUSR);
	auto proot = pop.root();

	{
		nvobj::transaction::manual tx(pop);

		proot->cons = nvobj::make_persistent<persistent_map_type>((uint64_t)HASH_POWER, 1);

		nvobj::transaction::commit();
	}

	auto map = pop.root()->cons;
	printf("initialization done.\n");
	printf("initial capacity %ld\n", map->capacity());


	// load benchmark files
	FILE *ycsb, *ycsb_read;
	char buf[1024];
	char *pbuf = buf;
	size_t len = 1024;
	size_t loaded = 0, inserted = 0, ins_failure = 0, found = 0, unfound = 0;
	size_t deleted = 0, del_existing = 0, updated = 0, upd_existing = 0;

	if ((ycsb = fopen(argv[2], "r")) == nullptr)
	{
		printf("failed to read %s\n", argv[2]);
		exit(1);
	}

	printf("Load phase begins \n");
    while (getline(&pbuf, &len, ycsb) != -1) {
		if (strncmp(buf, "INSERT", 6) == 0) {
			string_t key(buf + 7, KEY_LEN);
			auto ret = map->insert(persistent_map_type::value_type(key, key), (size_t)0);
			if (!ret.found) {
				loaded++;
			} else {
				break;
			}
		}
	}
	fclose(ycsb);
	printf("Load phase finishes: %ld items are inserted \n", loaded);

    // prepare data for the run phase
	if ((ycsb_read = fopen(argv[3], "r")) == NULL) {
		printf("fail to read %s\n", argv[3]);
		exit(1);
	}

	thread_queue* run_queue[thread_num];
	double* latency_queue[thread_num];
    int move[thread_num];
    for(size_t t = 0; t < thread_num; t ++){
        run_queue[t] = (thread_queue *)calloc(READ_WRITE_NUM / thread_num + 1, sizeof(thread_queue));
		latency_queue[t] = (double *)calloc(READ_WRITE_NUM / thread_num + 1, sizeof(double));
        move[t] = 0;
    }

	size_t operation_num = 0;
	while(getline(&pbuf,&len,ycsb_read) != -1){
		if(strncmp(buf, "INSERT", 6) == 0){
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].key = string_t(buf+7, KEY_LEN);
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].operation = level_hash_op::INSERT;
			move[operation_num%thread_num] ++;
		}
		else if(strncmp(buf, "READ", 4) == 0){
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].key = string_t(buf+5, KEY_LEN);
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].operation = level_hash_op::READ;
			move[operation_num%thread_num] ++;
		}
		else if (strncmp(buf, "DELETE", 6) == 0){
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].key = string_t(buf+7, KEY_LEN);
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].operation = level_hash_op::DELETE;
			move[operation_num%thread_num] ++;
		}
		else if (strncmp(buf, "UPDATE", 6) == 0){
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].key = string_t(buf+7, KEY_LEN);
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].operation = level_hash_op::UPDATE;
			move[operation_num%thread_num] ++;
		}
		operation_num ++;
	}
	fclose(ycsb_read);

	sub_thread* THREADS = (sub_thread*)malloc(sizeof(sub_thread)*thread_num);
    inserted = 0;

	printf("Run phase begins: %s \n", argv[3]);
    map->set_thread_num(thread_num);
    for(size_t t = 0; t < thread_num; t++){
        THREADS[t].id = t;
        THREADS[t].inserted = 0;
		THREADS[t].ins_failure = 0;
        THREADS[t].found = 0;
		THREADS[t].unfound = 0;
		THREADS[t].deleted = 0;
		THREADS[t].del_existing = 0;
		THREADS[t].updated = 0;
		THREADS[t].upd_existing = 0;
		THREADS[t].thread_num = thread_num;
		THREADS[t].run_queue = run_queue[t];
		THREADS[t].latency_queue = latency_queue[t];
    }

    std::vector<std::thread> threads;
    threads.reserve(thread_num);

	struct timespec start, end;
#ifdef LATENCY_ENABLE
	struct timespec stop;
#endif
    clock_gettime(CLOCK_MONOTONIC, &start);

	for (size_t i = 0; i < thread_num; i++)
	{
		threads.emplace_back([&](size_t thread_id) {
			printf("Thread %ld is opened\n", thread_id);
			size_t offset = loaded + READ_WRITE_NUM / thread_num * thread_id;
			for (size_t j = 0; j < READ_WRITE_NUM / thread_num; j++)
			{
				if (THREADS[thread_id].run_queue[j].operation == level_hash_op::INSERT)
				{
					auto ret = map->insert(persistent_map_type::value_type(
						THREADS[thread_id].run_queue[j].key,
						THREADS[thread_id].run_queue[j].key), thread_id);
					if (!ret.found)
					{
						THREADS[thread_id].inserted++;
					}
                    else
					{
						THREADS[thread_id].ins_failure++;
					}
				}
				else if (THREADS[thread_id].run_queue[j].operation == level_hash_op::READ)
				{
					auto ret = map->query(persistent_map_type::key_type(
						THREADS[thread_id].run_queue[j].key), thread_id);
					if (ret.found)
					{
						THREADS[thread_id].found++;
					}
					else
					{
						THREADS[thread_id].unfound++;
					}
				}
				else if (THREADS[thread_id].run_queue[j].operation == level_hash_op::DELETE)
				{
					auto ret = map->erase(persistent_map_type::key_type(
						THREADS[thread_id].run_queue[j].key), thread_id);
					THREADS[thread_id].deleted++;
					if (ret.found)
					{
						THREADS[thread_id].del_existing++;
					}
				}
				else if (THREADS[thread_id].run_queue[j].operation == level_hash_op::UPDATE)
				{
					string_t new_val = THREADS[thread_id].run_queue[j].key;
					new_val[0] = ~new_val[0];
					auto ret = map->update(persistent_map_type::value_type(
						THREADS[thread_id].run_queue[j].key, new_val),
						thread_id + 1);
					THREADS[thread_id].updated++;
					if (ret.found)
					{
						THREADS[thread_id].upd_existing++;
					}
				}
				else
				{
					printf("unknown level_hash_op\n");
					exit(1);
				}
#ifdef LATENCY_ENABLE
				clock_gettime(CLOCK_MONOTONIC, &stop);
				THREADS[thread_id].latency_queue[j] = stop.tv_sec * 1000000000.0 + stop.tv_nsec;
				assert(THREADS[thread_id].latency_queue[j] > 0);
#endif
			}
		}, i);
	}

	for (auto &t : threads) {
		t.join();
	}

	clock_gettime(CLOCK_MONOTONIC, &end);
	size_t elapsed = static_cast<size_t>((end.tv_sec - start.tv_sec) * 1000000000 +
		(end.tv_nsec - start.tv_nsec));

	for (size_t t = 0; t < thread_num; ++t) {
		inserted += THREADS[t].inserted;
		ins_failure += THREADS[t].ins_failure;
		found += THREADS[t].found;
		unfound += THREADS[t].unfound;
		deleted += THREADS[t].deleted;
		del_existing += THREADS[t].del_existing;
		updated += THREADS[t].updated;
		upd_existing += THREADS[t].upd_existing;
	}

	uint64_t total_slots = map->capacity();
	printf("capacity (after insertion) %ld, load factor %f\n",
		total_slots, (loaded + inserted) * 1.0 / total_slots);

	printf("Insert operations: %ld loaded, %ld inserted, %ld failed\n", loaded, inserted, ins_failure);
	printf("Read operations: %ld found, %ld not found\n", found, unfound);
	printf("Delete operations: deleted existing %ld items via %ld delete operations in total\n", del_existing, deleted);
	printf("Update operations: update existing %ld items via %ld update operations in total\n", upd_existing, updated);

	float elapsed_sec = elapsed / 1000000000.0;
	printf("%f seconds\n", elapsed_sec);
	printf("%f reqs per second (%ld threads)\n", READ_WRITE_NUM / elapsed_sec, thread_num);

	FILE *fp = fopen("throughput.txt", "w");
	fprintf(fp, "%f", READ_WRITE_NUM / elapsed_sec);
	fclose(fp);

#ifdef LATENCY_ENABLE
    double start_time = start.tv_sec * 1000000000.0 + start.tv_nsec;
    double latency = 0;
    double total_latency = 0;
#ifdef LIBPMEMOBJ_CPP_USE_TBB_RW_MUTEX
    FILE *fp_time = fopen("level_hash_tbb_processing_time.txt", "w");
	FILE *fp_latency = fopen("level_hash_tbb_latency.txt", "w");
	FILE *fp_reslut = fopen("latency.txt", "w");
#else
	FILE *fp_time = fopen("level_hash_processing_time.txt", "w");
	FILE *fp_latency = fopen("level_hash_latency.txt", "w");
	FILE *fp_reslut = fopen("latency.txt", "w");
#endif
    for (size_t t = 0; t < thread_num; ++t)
    {
        for (size_t i = 0; i < READ_WRITE_NUM / thread_num; i++)
        {
            latency = THREADS[t].latency_queue[i] - start_time;
            total_latency += latency;
            fprintf(fp_time, "%f\n", latency);
        }
    }
	printf("Average time: %f (ns)\n", total_latency * 1.0 / READ_WRITE_NUM);

	total_latency = 0;

	for (size_t t = 0; t < thread_num; ++t)
	{
		latency = THREADS[t].latency_queue[0] - start_time;
		total_latency += latency;
		fprintf(fp_latency, "%f\n", latency);

		for (size_t i = 1; i < READ_WRITE_NUM / thread_num; i++)
        {
            latency = THREADS[t].latency_queue[i] - THREADS[t].latency_queue[i-1];
            total_latency += latency;
            fprintf(fp_latency, "%f\n", latency);
        }
	}
    printf("Average latency: %f (ns)\n", total_latency * 1.0 / READ_WRITE_NUM);
	fprintf(fp_reslut, "%f", total_latency * 1.0 / READ_WRITE_NUM);
	fclose(fp_reslut);
#endif

	return 0;
}
