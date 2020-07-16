// #include "unittest.hpp"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <cassert>
#include <cstdio>
#include <iterator>
#include <sstream>
#include <thread>
#include <time.h>
#include <vector>

#include <libpmemobj++/experimental/concurrent_hash_map.hpp>
#include "polymorphic_string.h"

#define LAYOUT "concurrent_hash_map"

#define KEY_LEN 15
// #define LATENCY_ENABLE 1

#define RESERVE_BUCKET_NUM 20000


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

	// size_t operator()(string_view str) const
	// {
	// 	return hash(str.data(), str.size());
	// }

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
typedef nvobj::experimental::concurrent_hash_map<string_t, string_t, string_hasher>
	persistent_map_type;

struct root {
	nvobj::persistent_ptr<persistent_map_type> cons;
};

enum class cmap_op {
	UNKNOWN,
	INSERT,
	READ,

	MAX_OP
};

struct thread_queue {
	string_t key;
	cmap_op operation;
};

struct sub_thread {
	uint32_t id;
	uint64_t inserted;
	uint64_t found;
	uint64_t unfound;
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
	if (argc != 3) {
		printf("usage: %s <pool_path> <load_file>\n\n", argv[0]);
		printf("    pool_path: the pool file required for PMDK\n");
		printf("    load_file: an insert-only workload file\n");
		exit(1);
	}

	const char *path = argv[1];

	// initialize level hash
	nvobj::pool<root> pop;
	remove(path); // delete the mapped file.

	pop = nvobj::pool<root>::create(
		path, LAYOUT, PMEMOBJ_MIN_POOL * 2048, S_IWUSR | S_IRUSR);
	auto proot = pop.root();

	{
		nvobj::transaction::manual tx(pop);

		proot->cons = nvobj::make_persistent<persistent_map_type>();

		nvobj::transaction::commit();
	}

	printf("initialization done.\n");

	pop.root()->cons->reserve_bucket(RESERVE_BUCKET_NUM);

	printf("initial capacity %ld, #inserted %ld, #buckets %ld\n",
	       pop.root()->cons->capacity(),
	       pop.root()->cons->size(),
           pop.root()->cons->bucket_count());


	// load benchmark files
	auto map = pop.root()->cons;
	FILE *ycsb,*fout;
	char buf[1024];
	char *pbuf = buf;
	size_t len = 1024;
	size_t loaded = 0;

	if ((ycsb = fopen(argv[2], "r")) == nullptr)
	{
		printf("failed to read %s\n", argv[2]);
		exit(1);
	}

	printf("Load phase begins \n");
	fout = fopen("cmap_exp2_load_factor.csv", "w");//open a file to store the reslut
	fprintf(fout, "inserted,capacity,load_factor\n");
	while (getline(&pbuf, &len, ycsb) != -1) {
		if (strncmp(buf, "INSERT", 6) == 0) {
			string_t key(buf+7, KEY_LEN);
			bool ret = map->insert(persistent_map_type::value_type(key, key));
			if (ret) {
				loaded++;
			} else {
				std::cerr << "Error: found key " << key.c_str()
					<< " in the table, exit..." << std::endl;
				exit(1);
			}

			if (loaded % 10000 == 0)
			{
                size_t capacity = map->capacity();
				printf("loaded %ld items, #buckets %ld, size %ld, #capacity %ld, load fact0r %f\n",
                    loaded, map->bucket_count(), map->size(), capacity, loaded * 1.0 / capacity);
				fprintf(fout, "%ld,%ld,%f\n", map->size(), capacity, loaded * 1.0 / capacity);
			}
		}
	}
	fclose(ycsb);
	fclose(fout);
	printf("Load phase finishes: %ld items are inserted \n", loaded);

	return 0;
}
