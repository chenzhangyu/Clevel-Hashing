// #include "unittest.hpp"

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

#define DEBUG_RESIZING 1

#include "../../examples/libpmemobj_cpp_examples_common.hpp"
#include "../polymorphic_string.h"
#include "../profile.hpp"
#include <libpmemobj++/experimental/clevel_hash.hpp>

#define LAYOUT "clevel_hash"
#define KEY_LEN 15
// #define VALUE_LEN 16
// #define LATENCY_ENABLE 1

#define HASH_POWER 9

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
typedef nvobj::experimental::clevel_hash<string_t, string_t, string_hasher,
	std::equal_to<string_t>, HASH_POWER>
	persistent_map_type;

struct root {
	nvobj::persistent_ptr<persistent_map_type> cons;
};

enum class clevel_op {
	UNKNOWN,
	INSERT,
	READ,

	MAX_OP
};

struct thread_queue {
	string_t key;
	clevel_op operation;
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

	// initialize clevel hash
	nvobj::pool<root> pop;
	remove(path); // delete the mapped file.

	pop = nvobj::pool<root>::create(
		path, LAYOUT, PMEMOBJ_MIN_POOL * 1024, S_IWUSR | S_IRUSR);
	auto proot = pop.root();

	{
		nvobj::transaction::manual tx(pop);

		proot->cons = nvobj::make_persistent<persistent_map_type>();
		proot->cons->set_thread_num(1);

		nvobj::transaction::commit();
	}

	auto map = pop.root()->cons;
	printf("initialization done.\n");
	printf("initial capacity %ld\n", map->capacity());

	// load benchmark files
	FILE *ycsb, *fout;
	char buf[1024];
	char *pbuf = buf;
	size_t len = 1024;
	size_t loaded = 0;

	if ((ycsb = fopen(argv[2], "r")) == nullptr)
	{
		printf("failed to read %s\n", argv[2]);
		exit(1);
	}
	fout = fopen("clevel_hash_exp2_load_factor.csv", "w");

	printf("Load phase begins \n");
	fprintf(fout, "inserted,capacity,load_factor\n");
	while (getline(&pbuf, &len, ycsb) != -1) {
		if (strncmp(buf, "INSERT", 6) == 0) {
			string_t key(buf + 7, KEY_LEN);
			auto ret = map->insert(persistent_map_type::value_type(key, key), 1, loaded);
			if (!ret.found) {
				loaded++;
				// if (loaded % 10000 == 0)
				// 	std::cout << "[SUCCESS] inserted " << loaded
				// 		<< " in levels[" << ret.level_idx << "]"
				// 		<< " buckets[" << ret.bucket_idx << "]"
				// 		<< " slots[" << ret.slot_idx << "]"
				// 		<< " capacity: " << ret.capacity << std::endl;

				if (loaded % 10000 == 0)
				{
					std::cout << "Load factor: "
						<< (loaded - 1) * 1.0 / ret.capacity
						<< " inserted: " << loaded
						<< " capacity: " << ret.capacity << std::endl;
					fprintf(fout, "%ld,%ld,%f\n", loaded, ret.capacity,
						(loaded - 1) * 1.0 / ret.capacity);
				}
			} else {
				std::cerr << "Error: found key " << key.c_str()
					<< " in the table, exit..." << std::endl;
				exit(1);
			}
		}
	}
	fclose(ycsb);
	fclose(fout);
	printf("Load phase finishes: %ld items are inserted \n", loaded);

	pop.close();

	return 0;
}
