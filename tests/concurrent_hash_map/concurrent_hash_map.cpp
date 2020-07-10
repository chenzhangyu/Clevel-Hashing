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

// #include "unittest.hpp"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <iterator>
#include <thread>
#include <vector>
#include <sstream>
#include <time.h>
#include <cassert>
#include <cstdio>

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

template <typename Function>
void
parallel_exec(size_t concurrency, Function f)
{
	std::vector<std::thread> threads;
	threads.reserve(concurrency);

	for (size_t i = 0; i < concurrency; ++i) {
		threads.emplace_back(f, i);
	}

	for (auto &t : threads) {
		t.join();
	}
}

void
insert_test(nvobj::pool<root> &pop, size_t concurrency, size_t insert_num)
{
	auto map = pop.root()->cons;
	assert(map != nullptr);
	size_t insert_num_per_thread = insert_num / concurrency;

	map->initialize();

	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);

	parallel_exec(concurrency, [&](size_t thread_id) {
		size_t begin = thread_id * insert_num_per_thread;
		size_t end = begin + insert_num_per_thread;

		if (thread_id == (concurrency - 1))
		{
			end = insert_num;
		}

		for (size_t i = begin; i < end; i++)
		{
			bool res = map->insert(persistent_map_type::value_type(
				static_cast<int>(i), static_cast<int>(i)));
			assert(res);
		}
	});

	clock_gettime(CLOCK_MONOTONIC, &end);
	size_t elapsed = static_cast<size_t>((end.tv_sec - start.tv_sec) * 1000000000 +
		(end.tv_nsec - start.tv_nsec));

	printf("capacity (after insertion) %ld, load factor %f\n", map->bucket_count(),
		insert_num * 1.0 / map->bucket_count());

	float elapsed_sec = elapsed / 1000000000.0;
	printf("%f seconds\n", elapsed_sec);
	printf("%f reqs per second (%ld threads)\n", insert_num / elapsed_sec, concurrency);
}

void
lookup_test(nvobj::pool<root> &pop, size_t concurrency, size_t insert_num)
{
	auto map = pop.root()->cons;
	assert(map != nullptr);
	size_t insert_num_per_thread = insert_num / concurrency;

	map->initialize();

	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);

	parallel_exec(concurrency, [&](size_t thread_id) {
		size_t begin = thread_id * insert_num_per_thread;
		size_t end = begin + insert_num_per_thread;

		if (thread_id == (concurrency - 1))
		{
			end = insert_num;
		}

		for (size_t i = begin; i < end; i++)
		{
			persistent_map_type::accessor acc;
			bool res = map->find(acc, static_cast<int>(i));
			assert(res);
		}
	});

	clock_gettime(CLOCK_MONOTONIC, &end);
	size_t elapsed = static_cast<size_t>((end.tv_sec - start.tv_sec) * 1000000000 +
		(end.tv_nsec - start.tv_nsec));

	float elapsed_sec = elapsed / 1000000000.0;
	printf("%f seconds\n", elapsed_sec);
	printf("%f reqs per second (%ld threads)\n", insert_num / elapsed_sec, concurrency);
}

// void
// simple_test(nvobj::pool<root> &pop)
// {
// 	auto map = pop.root()->cons;

// 	UT_ASSERT(map != nullptr);

// 	map->initialize();

// 	bool ret = map->insert(persistent_map_type::value_type(123, 123));
// 	UT_ASSERT(ret == true);
// 	UT_ASSERT(map->count(123) == 1);
// 	UT_ASSERT(map->size() == 1);
// 	map->clear();

// 	UT_ASSERT(map->size() == 0);

// 	UT_ASSERT(std::distance(map->begin(), map->end()) == 0);
// }

// /*
//  * insert_and_lookup_test -- (internal) test insert and lookup operations
//  * pmem::obj::concurrent_hash_map<nvobj::p<int>, nvobj::p<int> >
//  */
// void
// insert_and_lookup_test(nvobj::pool<root> &pop)
// {
// 	const size_t NUMBER_ITEMS_INSERT = 50;

// 	// Adding more concurrency will increase DRD test time
// 	const size_t concurrency = 8;

// 	size_t TOTAL_ITEMS = NUMBER_ITEMS_INSERT * concurrency;

// 	auto map = pop.root()->cons;

// 	UT_ASSERT(map != nullptr);

// 	map->initialize();

// 	parallel_exec(concurrency, [&](size_t thread_id) {
// 		int begin = thread_id * NUMBER_ITEMS_INSERT;
// 		int end = begin + int(NUMBER_ITEMS_INSERT);
// 		for (int i = begin; i < end; ++i) {
// 			bool ret = map->insert(
// 				persistent_map_type::value_type(i, i));
// 			UT_ASSERT(ret == true);

// 			UT_ASSERT(map->count(i) == 1);

// 			persistent_map_type::accessor acc;
// 			bool res = map->find(acc, i);
// 			UT_ASSERT(res == true);
// 			UT_ASSERT(acc->first == i);
// 			UT_ASSERT(acc->second == i);
// 			acc->second.get_rw() += 1;
// 			pop.persist(acc->second);
// 		}

// 		for (int i = begin; i < end; ++i) {
// 			persistent_map_type::const_accessor const_acc;
// 			bool res = map->find(const_acc, i);
// 			UT_ASSERT(res == true);
// 			UT_ASSERT(const_acc->first == i);
// 			UT_ASSERT(const_acc->second == i + 1);
// 		}
// 	});

// 	UT_ASSERT(map->size() == TOTAL_ITEMS);

// 	UT_ASSERT(std::distance(map->begin(), map->end()) == int(TOTAL_ITEMS));

// 	map->rehash(TOTAL_ITEMS * 8);

// 	UT_ASSERT(map->size() == TOTAL_ITEMS);

// 	UT_ASSERT(std::distance(map->begin(), map->end()) == int(TOTAL_ITEMS));

// 	size_t buckets = map->bucket_count();

// 	map->initialize(true);

// 	UT_ASSERT(map->bucket_count() == buckets);

// 	UT_ASSERT(map->size() == TOTAL_ITEMS);

// 	map->initialize();

// 	UT_ASSERT(map->bucket_count() == buckets);

// 	UT_ASSERT(map->size() == TOTAL_ITEMS);

// 	map->clear();

// 	UT_ASSERT(map->size() == 0);

// 	UT_ASSERT(std::distance(map->begin(), map->end()) == 0);
// }

// /*
//  * insert_and_erase_test -- (internal) test insert and erase operations
//  * pmem::obj::concurrent_hash_map<nvobj::p<int>, nvobj::p<int> >
//  */
// void
// insert_and_erase_test(nvobj::pool<root> &pop)
// {
// 	const size_t NUMBER_ITEMS_INSERT = 50;

// 	// Adding more concurrency will increase DRD test time
// 	const size_t concurrency = 8;

// 	auto map = pop.root()->cons;

// 	UT_ASSERT(map != nullptr);

// 	map->initialize();

// 	parallel_exec(concurrency, [&](size_t thread_id) {
// 		int begin = thread_id * NUMBER_ITEMS_INSERT;
// 		int end = begin + int(NUMBER_ITEMS_INSERT) / 2;
// 		for (int i = begin; i < end; ++i) {
// 			bool res = map->insert(
// 				persistent_map_type::value_type(i, i));
// 			UT_ASSERT(res == true);

// 			res = map->erase(i);
// 			UT_ASSERT(res == true);

// 			UT_ASSERT(map->count(i) == 0);
// 		}
// 	});

// 	UT_ASSERT(map->size() == 0);
// }

// /*
//  * insert_and_erase_test -- (internal) test insert and erase operations
//  * pmem::obj::concurrent_hash_map<nvobj::p<int>, nvobj::p<int> >
//  */
// void
// insert_erase_lookup_test(nvobj::pool<root> &pop)
// {
// 	const size_t NUMBER_ITEMS_INSERT = 50;

// 	// Adding more concurrency will increase DRD test time
// 	const size_t concurrency = 4;

// 	auto map = pop.root()->cons;

// 	UT_ASSERT(map != nullptr);

// 	map->initialize();

// 	std::vector<std::thread> threads;
// 	threads.reserve(concurrency);

// 	for (size_t i = 0; i < concurrency; ++i) {
// 		threads.emplace_back([&]() {
// 			for (int i = 0;
// 			     i < static_cast<int>(NUMBER_ITEMS_INSERT); ++i) {
// 				map->insert(
// 					persistent_map_type::value_type(i, i));
// 			}
// 		});
// 	}

// 	for (size_t i = 0; i < concurrency; ++i) {
// 		threads.emplace_back([&]() {
// 			for (int i = 0;
// 			     i < static_cast<int>(NUMBER_ITEMS_INSERT); ++i) {
// 				map->erase(i);
// 			}
// 		});
// 	}

// 	for (size_t i = 0; i < concurrency; ++i) {
// 		threads.emplace_back([&]() {
// 			for (int i = 0;
// 			     i < static_cast<int>(NUMBER_ITEMS_INSERT); ++i) {
// 				persistent_map_type::accessor acc;
// 				bool res = map->find(acc, i);

// 				if (res) {
// 					UT_ASSERTeq(acc->first, i);
// 					UT_ASSERT(acc->second >= i);
// 					acc->second.get_rw() += 1;
// 					pop.persist(acc->second);
// 				}
// 			}
// 		});
// 	}

// 	for (auto &t : threads) {
// 		t.join();
// 	}

// 	for (auto &e : *map) {
// 		UT_ASSERT(e.first <= e.second);
// 	}
// }
}

int
main(int argc, char *argv[])
{
	// parse inputs
	if (argc != 4) {
		printf("usage: %s <file_name> <insert_num> <thread_num>\n", argv[0]);
		exit(1);
	}

	const char *path = argv[1];
	size_t insert_num;
	size_t concurrency;

	std::stringstream s;
	s << argv[2] << " " << argv[3];
	s >> insert_num >> concurrency;

	nvobj::pool<root> pop;

	try {
		pop = nvobj::pool<root>::create(
			path, LAYOUT, PMEMOBJ_MIN_POOL * 20, S_IWUSR | S_IRUSR);
		nvobj::make_persistent_atomic<persistent_map_type>(
			pop, pop.root()->cons);
	} catch (pmem::pool_error &pe) {
		printf("!pool::create: %s %s\n", pe.what(), path);
		exit(1);
	}

	printf("initialization done.\n");

	// Reserve at least the same capacity with level hash (default level_size = 12)
	// pop.root()->cons->reserve_bucket(24576);    // (2^12 + 2^11) * 4 = 24576
	pop.root()->cons->reserve_bucket(98304);  // (2^14 + 2^13) * 4 = 98304 
	// pop.root()->cons->reserve_bucket(1572864);  // (2^18 + 2^17) * 4 = 1572864

	printf("initial capacity %ld, #inserted %ld\n",
	       pop.root()->cons->bucket_count(),
	       pop.root()->cons->size());

	// simple_test(pop);

	// insert_and_lookup_test(pop);

	// insert_and_erase_test(pop);

	// insert_erase_lookup_test(pop);

	// start benchmark
	printf("start concurrent insertion...\n");
	insert_test(pop, concurrency, insert_num);

	printf("start concurrent lookup...\n");
	lookup_test(pop, concurrency, insert_num);

	pop.close();

	return 0;
}
