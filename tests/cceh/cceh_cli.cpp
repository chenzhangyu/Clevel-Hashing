#include "unittest.hpp"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <iterator>
#include <thread>
#include <vector>
#include <cstdio>

#include "../../examples/libpmemobj_cpp_examples_common.hpp"
#include <libpmemobj++/experimental/cceh.hpp>

#define LAYOUT "CCEH"
#define KEY_LEN 15
#define VALUE_LEN 16

namespace nvobj = pmem::obj;

namespace
{

typedef nvobj::experimental::CCEH
	persistent_map_type;

struct root {
	nvobj::persistent_ptr<persistent_map_type> cons;
};

enum class cceh_op {
	UNKNOWN,
	PRINT,
	ALLOC,

	MAX_OP
};

cceh_op
parse_cceh_op(const char *str)
{
	if (strcmp(str, "print") == 0)
		return cceh_op::PRINT;
	else if (strcmp(str, "alloc") == 0)
		return cceh_op::ALLOC;
	else
		return cceh_op::UNKNOWN;
}

void
insert_item(nvobj::pool<root> &pop, int i)
{
	auto map = pop.root()->cons;
	UT_ASSERT(map != nullptr);

	uint8_t key[KEY_LEN] = {0};
	uint8_t value[VALUE_LEN] = {0};

	snprintf(reinterpret_cast<char *>(key), KEY_LEN, "%d", i);
	snprintf(reinterpret_cast<char *>(value), VALUE_LEN, "%d", i);

	auto r = map->insert(key, value, KEY_LEN, VALUE_LEN, i);

	if (r.found)
	{
		UT_OUT("[SUCCESS] inserted %d in segment[%d] buckets[%ld]",
			i, r.segment_idx, r.bucket_idx);
	}
	else
	{
		UT_OUT("[FAIL] can not insert %d", i);
	}

}

void
search_item(nvobj::pool<root> &pop, int i)
{
	auto map = pop.root()->cons;
	UT_ASSERT(map != nullptr);

	uint8_t key[KEY_LEN] = {0};

	snprintf(reinterpret_cast<char *>(key), KEY_LEN, "%d", i);

	auto r = map->get(key, KEY_LEN);

	if (r.found)
	{
		UT_OUT("[SUCCESS] found %d in segment[%d] buckets[%ld]",
			i, r.segment_idx, r.bucket_idx);
	}
	else
	{
		UT_OUT("[FAIL] can not find %d", i);
	}
}

void
print_usage(char *exe)
{
	UT_OUT("usage: %s <pool_path> <cmd> <key>\n", exe);
	UT_OUT("    pool_path: the pool file required for PMDK");
	UT_OUT("    cmd: a query for a key, including \"print\" (search) and \"alloc\" (insert)");
	UT_OUT("    key: a key (integer) required for the query\n");
}

void
simple_test(nvobj::pool<root> &pop)
{
	// const size_t level_size = 12;

	auto map = pop.root()->cons;

	UT_ASSERT(map != nullptr);
	map->foo();

}
} /* Annoymous namespace */

int
main(int argc, char *argv[])
{
	START();

	if (argc != 4) {
		print_usage(argv[0]);
		UT_FATAL("Illegal arguments!");
	}

	const char *path = argv[1];

	nvobj::pool<root> pop;

	if (file_exists(path) != 0)
	{
		pop = nvobj::pool<root>::create(
			path, LAYOUT, PMEMOBJ_MIN_POOL * 20, S_IWUSR | S_IRUSR);
		auto proot = pop.root();

		nvobj::transaction::manual tx(pop);

		proot->cons = nvobj::make_persistent<persistent_map_type>(2U);

		nvobj::transaction::commit();
	}
	else
	{
		pop = nvobj::pool<root>::open(path, LAYOUT);
	}

	cceh_op op = parse_cceh_op(argv[2]);

	int key = atoi(argv[3]);

	switch (op)
	{
		case cceh_op::PRINT:
			search_item(pop, key);
			break;

		case cceh_op::ALLOC:
			insert_item(pop, key);
			break;

		default:
			simple_test(pop);
			print_usage(argv[0]);
			break;
	}

	pop.close();

	return 0;
}