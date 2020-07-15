#include "unittest.hpp"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <cstdio>
#include <iterator>
#include <thread>
#include <vector>

#include "../../examples/libpmemobj_cpp_examples_common.hpp"
#include <libpmemobj++/experimental/level_hash.hpp>

#define LAYOUT "level_hash"

namespace nvobj = pmem::obj;

namespace
{
typedef nvobj::experimental::level_hash<nvobj::p<int>, nvobj::p<int>>
	persistent_map_type;

struct root {
	nvobj::persistent_ptr<persistent_map_type> cons;
};

enum class level_hash_op {
	UNKNOWN,
	PRINT,
	FREE,
	ALLOC,

	MAX_OP
};

level_hash_op
parse_level_hash_op(const char *str)
{
	if (strcmp(str, "print") == 0)
		return level_hash_op::PRINT;
	else if (strcmp(str, "free") == 0)
		return level_hash_op::FREE;
	else if (strcmp(str, "alloc") == 0)
		return level_hash_op::ALLOC;
	else
		return level_hash_op::UNKNOWN;
}

void
insert_item(nvobj::pool<root> &pop, int i)
{
	auto map = pop.root()->cons;
	UT_ASSERT(map != nullptr);

	auto r = map->insert(persistent_map_type::value_type(i, i), (size_t)0);

	if (!r.found)
	{
		UT_OUT("[SUCCESS] inserted %d", i);
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

	auto r = map->query(persistent_map_type::key_type(i), (size_t)0);

	if (r.found)
	{
		UT_OUT("[SUCCESS] found %d in levels[%ld] buckets[%ld] slots[%d]",
			i, r.level_idx, r.bucket_idx, r.slot_idx);
	}
	else
	{
		UT_OUT("[FAIL] can not find %d", i);
	}
}

void
delete_item(nvobj::pool<root> &pop, int i)
{
	auto map = pop.root()->cons;
	UT_ASSERT(map != nullptr);

	auto r = map->erase(persistent_map_type::key_type(i), (size_t)0);

	if (r.found)
	{
		UT_OUT("[SUCCESS] delete %d in levels[%ld] buckets[%ld] slots[%d]",
			i, r.level_idx, r.bucket_idx, r.slot_idx);
	}
	else
	{
		UT_OUT("[FAIL] can not delete %d", i);
	}
}

void
print_usage(char *exe)
{
	UT_OUT("usage: %s <pool_path> <cmd> <key>\n", exe);
	UT_OUT("    pool_path: the pool file required for PMDK");
	UT_OUT("    cmd: a query for a key, including \"print\" (search), \"alloc\" (insert), and \"free\" (delete)");
	UT_OUT("    key: a key (integer) required for the query\n");
}
}

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

		proot->cons = nvobj::make_persistent<persistent_map_type>((size_t)12, 1);

		nvobj::transaction::commit();
	}
	else
	{
		pop = nvobj::pool<root>::open(path, LAYOUT);
	}

	level_hash_op op = parse_level_hash_op(argv[2]);

	int key = atoi(argv[3]);

	switch (op)
	{
		case level_hash_op::PRINT:
			search_item(pop, key);
			break;

		case level_hash_op::ALLOC:
			insert_item(pop, key);
			break;

		case level_hash_op::FREE:
			delete_item(pop, key);
			break;

		default:
			print_usage(argv[0]);
			break;
	}

	pop.close();

	return 0;
}
