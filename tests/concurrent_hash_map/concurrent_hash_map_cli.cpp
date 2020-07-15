/*
 * Copyright 2018-2019, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * concurrent_hash_map.cpp -- pmem::obj::concurrent_hash_map test
 *
 */

#include "unittest.hpp"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <cstring>
#include <iostream>
#include <iterator>
#include <thread>
#include <vector>

#include "../../examples/libpmemobj_cpp_examples_common.hpp"
#include <libpmemobj++/experimental/concurrent_hash_map.hpp>

#define LAYOUT "concurrent_hash_map"

namespace nvobj = pmem::obj;

namespace
{

typedef nvobj::experimental::concurrent_hash_map<nvobj::p<int>, nvobj::p<int>>
	persistent_map_type;

struct root {
	nvobj::persistent_ptr<persistent_map_type> cons;
};

enum class cmap_op {
	UNKNOWN,
	PRINT,
	FREE,
	ALLOC,

	MAX_OP
};

cmap_op
parse_cmap_op(const char *str)
{
	if (strcmp(str, "print") == 0)
		return cmap_op::PRINT;
	else if (strcmp(str, "free") == 0)
		return cmap_op::FREE;
	else if (strcmp(str, "alloc") == 0)
		return cmap_op::ALLOC;
	else
		return cmap_op::UNKNOWN;
}

void
insert_item(nvobj::pool<root> &pop, int i)
{
	UT_OUT("insert key %d", i);

	auto map = pop.root()->cons;
	UT_ASSERT(map != nullptr);

	bool ret = map->insert(persistent_map_type::value_type(i, i));
	UT_ASSERT(ret == true);

	UT_ASSERT(map->count(i) == 1);
}

void delete_item(nvobj::pool<root> &pop, int i)
{
	UT_OUT("delete key %d", i);

	auto map = pop.root()->cons;
	UT_ASSERT(map != nullptr);

	bool ret = map->erase(i);
	UT_ASSERT(ret == true);

	UT_ASSERT(map->count(i) == 0);
}

void search_item(nvobj::pool<root> &pop, int i)
{
	UT_OUT("search key %d", i);

	auto map = pop.root()->cons;
	UT_ASSERT(map != nullptr);

	persistent_map_type::accessor acc;
	bool ret = map->find(acc, i);
	if (ret)
	{
		UT_OUT("found! key = %d, value = %d", i, *acc);
	}

	else
	{
		UT_OUT("not found value for key = %d", i);
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
		nvobj::make_persistent_atomic<persistent_map_type>(
			pop, pop.root()->cons);
	}
	else
	{
		pop = nvobj::pool<root>::open(path, LAYOUT);
	}

	cmap_op op = parse_cmap_op(argv[2]);

	int key = atoi(argv[3]);

	switch (op)
	{
		case cmap_op::PRINT:
			search_item(pop, key);
			break;

		case cmap_op::ALLOC:
			insert_item(pop, key);
			break;

		case cmap_op::FREE:
			delete_item(pop, key);
			break;

		default:
			print_usage(argv[0]);
			break;
	}

	pop.close();

	return 0;
}
