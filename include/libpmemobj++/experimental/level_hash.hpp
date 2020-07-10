#ifndef PMEMOBJ_LEVEL_HASH_HPP
#define PMEMOBJ_LEVEL_HASH_HPP

#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/detail/template_helpers.hpp>
#include <libpmemobj++/detail/compound_pool_ptr.hpp>
#include <libpmemobj++/experimental/v.hpp>
#include <libpmemobj++/experimental/concurrent_hash_map.hpp>
#include <libpmemobj++/experimental/hash.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

#if LIBPMEMOBJ_CPP_USE_TBB_RW_MUTEX
#include "tbb/spin_rw_mutex.h"
#else
#include <libpmemobj++/shared_mutex.hpp>
#endif

#include <libpmemobj++/detail/persistent_pool_ptr.hpp>
#include <libpmemobj++/detail/specialization.hpp>

#include <atomic>
#include <cassert>
#include <functional>
#include <initializer_list>
#include <iterator> 
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <cstdint>
#include <cstdlib>
#include <time.h>
#include <cmath>
#include <iostream>
#include <pthread.h>

#if _MSC_VER
#include <intrin.h>
#include <windows.h>
#endif

namespace pmem
{
namespace obj
{
namespace experimental
{

using namespace pmem::obj;

#if !LIBPMEMOBJ_CPP_USE_TBB_RW_MUTEX
using internal::shared_mutex_scoped_lock;
#endif

template <typename Key, typename T, typename Hash = std::hash<Key>,
	typename KeyEqual = std::equal_to<Key>>
class level_hash {
public:
	using key_type = Key;
	using mapped_type = T;
	using value_type = std::pair<const Key, T>;
	using size_type = size_t;
	using difference_type = ptrdiff_t;
	using pointer = value_type *;
	using const_pointer = const value_type *;
	using reference = value_type &;
	using const_reference = const value_type &;

	using hasher = Hash;
	using key_equal =
		typename internal::key_equal_type<Hash, KeyEqual>::type;
	using hv_type = uint64_t;

#if LIBPMEMOBJ_CPP_USE_TBB_RW_MUTEX
	using mutex_t = pmem::obj::experimental::v<tbb::spin_rw_mutex>;
	using scoped_t = tbb::spin_rw_mutex::scoped_lock;
#else
	using mutex_t = pmem::obj::shared_mutex;
	using scoped_t = shared_mutex_scoped_lock;
#endif

	using kv_ptr_t = detail::compound_pool_ptr<value_type>;

	constexpr static size_type assoc_num = 4;

	// protected:
	hv_type
	first_hash(const key_type &key) const
	{
#ifdef KEY_LEN
		return internal::hash(key.c_str(), KEY_LEN, f_seed);
#else
		return hasher{}(key);
#endif
	}

	hv_type
	second_hash(const key_type &key) const
	{
#ifdef KEY_LEN

		return internal::hash(key.c_str(), KEY_LEN, s_seed);
#else
		hv_type hv = hasher{}(key);
		hv = (hv >> 1 << 1) + 1;
		// 0xc6a4a7935bd1e995 is the hash constant from 64-bit MurmurHash2
		return (hv_type)(hv * 0xc6a4a7935bd1e995);
#endif
	}

	difference_type
	first_index(hv_type hv, size_type capacity) const
	{
		// Since the "bucket_idx" needs to be "std::ptrdiff_t" due to
		// the requirement in persistent_ptr<bucket[]>, so we adopt
		// "difference_type" (i.e., "std::ptrdiff_t") as the data type
		// of bucket index.
		return static_cast<difference_type>(hv % (capacity / 2));
	}

	difference_type
	second_index(hv_type hv, size_type capacity) const
	{
		return static_cast<difference_type>(hv % (capacity / 2) + capacity / 2);
	}

	class accessor;
	class const_accessor;

	struct barrier
	{
		pthread_cond_t complete;
		pthread_mutex_t mutex;
		int count;
		int crossing;

		barrier(int n)
		{
			pthread_cond_init(&complete, nullptr);
			pthread_mutex_init(&mutex, nullptr);
			count = n;
			crossing = 0;
		}

		void
		cross(level_hash *ht, size_type thread_id)
		{
			pthread_mutex_lock(&mutex);
			crossing++;
			if (crossing < count)
			{
				std::cout << "[Thread-" << thread_id << "] adds crossing to " << crossing << std::endl;
				pthread_cond_wait(&complete, &mutex);
				std::cout << "[Thread-" << thread_id << "] crosses barrier" << std::endl;
			}
			else
			{
				std::cout << "[Thread-" << thread_id << "] expands, crossing = " << crossing << std::endl;
				ht->expand();
				pthread_cond_broadcast(&complete);
				std::cout << "[Thread-" << thread_id << "] broadcasts" << std::endl;
				crossing = 0;
			}
			pthread_mutex_unlock(&mutex);
		}
	};

	struct ret
	{
		bool found;
		uint8_t level_idx;
		difference_type bucket_idx;
		int8_t slot_idx;
		bool expanded;
		uint64_t capacity;

		ret(size_type _level_idx, difference_type _bucket_idx,
			size_type _slot_idx, bool _expanded=false, uint64_t _cap = 0)
		    : found(true), level_idx(_level_idx), bucket_idx(_bucket_idx),
			slot_idx(_slot_idx), expanded(_expanded), capacity(_cap)
		{
		}

		ret(bool _expanded, uint64_t _cap)
			: found(false), level_idx(0), bucket_idx(0), slot_idx(0),
			expanded(_expanded), capacity(_cap)
		{
		}

		ret(bool _found) : found(_found), level_idx(0), bucket_idx(0),
			slot_idx(0), expanded(false), capacity(0)
		{
		}

		ret() : found(false), level_idx(0), bucket_idx(0), slot_idx(0),
			expanded(false), capacity(0)
		{
		}
	};

	struct bucket
	{
		kv_ptr_t slots[assoc_num];
		uint8_t tokens[assoc_num];
	};

	struct level
	{
		persistent_ptr<bucket[]> buckets;
		persistent_ptr<persistent_ptr<mutex_t[assoc_num]>[]> mutexes;

		p<uint64_t> capacity;

		void
		clear()
		{
			if (buckets)
			{
				delete_persistent<bucket[]>(buckets, capacity);
				buckets = nullptr;
			}

			if (mutexes)
			{
				for (size_type i = 0; i < capacity; i++)
				{
					difference_type di = static_cast<difference_type>(i);
					delete_persistent<mutex_t[assoc_num]>(mutexes[di]);
				}
				delete_persistent<persistent_ptr<mutex_t[assoc_num]>[]>(mutexes, capacity);
			}
		}
	};

	level_hash(size_type level_power, size_type n_threads)
	    : num_threads(n_threads), resize_barrier(n_threads)
	{
		std::cout << "level_hash constructor, level_size = "
			<< level_power << std::endl;

		assert(level_power > 0);
		level_size.get_rw() = level_power;
		level_expand_time = 0;

		std::cout << "level_size : " << level_size << std::endl;

		/* Generate two random seeds */
		srand(time(nullptr));
		do
		{
			f_seed = static_cast<uint64_t>(rand());
			s_seed = static_cast<uint64_t>(rand());
			f_seed = f_seed << (rand() % 63);
			s_seed = s_seed << (rand() % 63);
		} while (f_seed == s_seed);

		PMEMoid oid = pmemobj_oid(this);

		assert(!OID_IS_NULL(oid));

		my_pool_uuid = oid.pool_uuid_lo;

		for (size_t i = 0; i < 2; i++)
		{
			levels[i] = make_persistent<level>();
			levels[i]->capacity = pow(2, level_size - i);
			levels[i]->buckets = make_persistent<bucket[]>(levels[i]->capacity);
			levels[i]->mutexes =
				make_persistent<persistent_ptr<mutex_t[assoc_num]>[]>(levels[i]->capacity);

			for (difference_type idx = 0;
			     idx < (difference_type)levels[i]->capacity; idx++)
				levels[i]->mutexes[idx] = make_persistent<mutex_t[assoc_num]>();
		}

		interim_level = nullptr;
		need_resizing.store(false);
	}

	// only used for testing and in single-thread mode
	void
	set_thread_num(size_type num)
	{
		num_threads = num;
		resize_barrier.count = num_threads;
		std::cout << "switch to " << num_threads << "-thread mode" << std::endl;
	}

	static void
	allocate_KV_copy_construct(pool_base &pop,
		persistent_ptr<value_type> &KV_ptr,
		const void *param)
	{
		const value_type *v = static_cast<const value_type *>(param);
		internal::make_persistent_object<value_type>(pop, KV_ptr, *v);
	}

	static void
	allocate_KV_move_construct(pool_base &pop,
		persistent_ptr<value_type> &KV_ptr,
		const void *param)
	{
		const value_type *v = static_cast<const value_type *>(param);
		internal::make_persistent_object<value_type>(
			pop, KV_ptr, std::move(*const_cast<value_type *>(v)));
	}

	// Corresponding to "level_insert" in C version
	// bool
	ret
	insert(const value_type &value, size_type thread_id)
	{
		return generic_insert(value.first, &value,
			allocate_KV_copy_construct, thread_id);
	}

	ret
	insert(value_type &&value, size_type thread_id)
	{
		return generic_insert(value.first, &value,
			allocate_KV_move_construct, thread_id);
	}

	ret
	update(const value_type &value, size_type thread_id)
	{
		return generic_update(value.first, &value,
			allocate_KV_copy_construct, thread_id);
	}

	ret
	update(value_type &&value, size_type thread_id)
	{
		return generic_update(value.first, &value,
			allocate_KV_move_construct, thread_id);
	}

	ret
	generic_insert(const key_type &key, const void *param,
		void (*allocate_KV)(
			pool_base &, persistent_ptr<value_type> &, const void *),
		size_type thread_id)
	{
		difference_type f_idx, s_idx;
		hv_type f_hv = first_hash(key);
		hv_type s_hv = second_hash(key);

		pool_base pop = get_pool_base();

		persistent_ptr<value_type> tmp_entry;
		allocate_KV(pop, tmp_entry, param);
		kv_ptr_t created(tmp_entry.raw().off);

		bool expanded = false;
		uint64_t initial_capacity = 0;

		while (true)
		{
			if (need_resizing.load())
				resize_barrier.cross(this, thread_id);

#ifdef DEBUG_RESIZING
			if (initial_capacity == 0)
				initial_capacity = capacity();
#endif

			// 1. Find empty slots.
			for (size_t i = 0; i < 2; i++)
			{
				f_idx = first_index(f_hv, levels[i]->capacity);
				s_idx = second_index(s_hv, levels[i]->capacity);

				for (size_t j = 0; j < assoc_num; j++)
				{
					scoped_t slot_guard(levels[i]->mutexes[f_idx][j], true);
					if (levels[i]->buckets[f_idx].tokens[j] ==0)
					{
						insert_empty_slot(pop, i, f_idx, j, created);
						return ret(expanded, initial_capacity);
					}
					slot_guard.release();
					slot_guard.acquire(levels[i]->mutexes[s_idx][j], true);
					if (levels[i]->buckets[s_idx].tokens[j] == 0)
					{
						insert_empty_slot(pop, i, s_idx, j, created);
						return ret(expanded, initial_capacity);
					}
					slot_guard.release();
				}
			}


			// 2. Try one-step cuckoo displacements in the same level.
			for (size_type i = 0; i < 2; i++)
			{
				f_idx = first_index(f_hv, levels[i]->capacity);
				s_idx = second_index(s_hv, levels[i]->capacity);

				if (try_movement(pop, i, f_idx, created))
					return ret(expanded, initial_capacity);

				if (try_movement(pop, i, s_idx, created))
					return ret(expanded, initial_capacity);
			}


			// 3. Try bottom-to-top movement.
			if (level_expand_time > 0)
			{
				// f_idx and s_idx were calculated using the capacity of bottom level
				if (b2t_movement(pop, f_idx, created))
					// return true;
					return ret(expanded, initial_capacity);

				if (b2t_movement(pop, s_idx, created))
					// return true;
					return ret(expanded, initial_capacity);
			}

			expanded = true;
			need_resizing.store(true);
		}
	}

	// Corresponding to "level_static_query" in C version
	// mapped_type
	ret
	query(const key_type &key, size_type thread_id)
	{
		if (need_resizing.load())
			resize_barrier.cross(this, thread_id);

		difference_type f_idx, s_idx;
		hv_type f_hv = first_hash(key);
		hv_type s_hv = second_hash(key);

		for (size_type i = 0; i < 2; i++)
		{
			f_idx = first_index(f_hv, levels[i]->capacity);
			s_idx = second_index(s_hv, levels[i]->capacity);

			for (size_type j = 0; j < assoc_num; j++)
			{
				scoped_t slot_guard(levels[i]->mutexes[f_idx][j], false);
				if (levels[i]->buckets[f_idx].tokens[j] == 1 &&
					key_equal{}(levels[i]->buckets[f_idx].slots[j].get_address(my_pool_uuid)->first, key))
				{
					return ret(i, f_idx, j);
				}
			}

			for (size_type j = 0; j < assoc_num; j++)
			{
				scoped_t slot_guard(levels[i]->mutexes[s_idx][j], false);
				if (levels[i]->buckets[s_idx].tokens[j] == 1 &&
					key_equal{}(levels[i]->buckets[s_idx].slots[j].get_address(my_pool_uuid)->first, key))
				{
					return ret(i, s_idx, j);
				}
			}
		}

		// return nullptr;
		return ret();
	}

	// Correspond to "level_delete" in C version
	// bool
	ret
	erase(const key_type &key, size_type thread_id)
	{
		if (need_resizing.load())
			resize_barrier.cross(this, thread_id);

		difference_type f_idx, s_idx;
		hv_type f_hv = first_hash(key);
		hv_type s_hv = second_hash(key);

		pool_base pop = get_pool_base();

		for (size_type i = 0; i < 2; i++)
		{
			f_idx = first_index(f_hv, levels[i]->capacity);
			s_idx = second_index(s_hv, levels[i]->capacity);

			for (size_type j = 0; j < assoc_num; j++)
			{
				scoped_t slot_guard(levels[i]->mutexes[f_idx][j], true);
				if (levels[i]->buckets[f_idx].tokens[j] == 1 &&
					key_equal{}(levels[i]->buckets[f_idx].slots[j].get_address(my_pool_uuid)->first, key))
				{
					levels[i]->buckets[f_idx].tokens[j] = 0;
					pop.persist(&levels[i]->buckets[f_idx].tokens[j], sizeof(uint8_t));

					return ret(i, f_idx, j);
				}
			}

			for (size_type j = 0; j < assoc_num; j++)
			{
				scoped_t slot_guard(levels[i]->mutexes[s_idx][j], true);
				if (levels[i]->buckets[s_idx].tokens[j] == 1 &&
					key_equal{}(levels[i]->buckets[s_idx].slots[j].get_address(my_pool_uuid)->first, key))
				{
					levels[i]->buckets[s_idx].tokens[j] = 0;
					pop.persist(&levels[i]->buckets[s_idx].tokens[j], sizeof(uint8_t));

					return ret(i, s_idx, j);
				}
			}
		}

		// return false;
		return ret();
	}

	ret
	generic_update(const key_type &key, const void *param,
		void (*allocate_KV)(
			pool_base &, persistent_ptr<value_type> &, const void *),
		size_type thread_id)
	{
		difference_type f_idx, s_idx;
		hv_type f_hv = first_hash(key);
		hv_type s_hv = second_hash(key);

		pool_base pop = get_pool_base();

		persistent_ptr<value_type> tmp_entry;
		allocate_KV(pop, tmp_entry, param);
		kv_ptr_t created(tmp_entry.raw().off);

		for (size_type i = 0; i < 2; i++)
		{
			f_idx = first_index(f_hv, levels[i]->capacity);
			s_idx = second_index(s_hv, levels[i]->capacity);

			for (size_type j = 0; j < assoc_num; j++)
			{
				scoped_t slot_guard(levels[i]->mutexes[f_idx][j], false);
				if (levels[i]->buckets[f_idx].tokens[j] == 1 &&
					key_equal{}(levels[i]->buckets[f_idx].slots[j].get_address(my_pool_uuid)->first, key))
				{
					levels[i]->buckets[f_idx].slots[j] = created;
					pop.persist(&levels[i]->buckets[f_idx].slots[j].off, sizeof(kv_ptr_t));
					levels[i]->buckets[f_idx].tokens[j] = 1;
					pop.persist((&levels[i]->buckets[f_idx].tokens[j]), sizeof(uint8_t));
					return ret(i, f_idx, j);
				}
			}

			for (size_type j = 0; j < assoc_num; j++)
			{
				scoped_t slot_guard(levels[i]->mutexes[s_idx][j], false);
				if (levels[i]->buckets[s_idx].tokens[j] == 1 &&
					key_equal{}(levels[i]->buckets[s_idx].slots[j].get_address(my_pool_uuid)->first, key))
				{
					levels[i]->buckets[s_idx].slots[j] = created;
					pop.persist(&levels[i]->buckets[s_idx].slots[j].off, sizeof(kv_ptr_t));
					levels[i]->buckets[s_idx].tokens[j] = 1;
					pop.persist((&levels[i]->buckets[s_idx].tokens[j]), sizeof(uint8_t));
					return ret(i, s_idx, j);
				}
			}
		}

		// return nullptr;
		return ret();
	}

	void
	expand()
	{
		std::cout << "start expanding..." << std::endl;
		assert(levels);

		pool_base pop = get_pool_base();

		{

			// The current transaction state for a new tx should be
			// "TX_STAGE_NONE" or "TX_STAGE_WORK".
			// assert(pmemobj_tx_stage() == TX_STAGE_NONE ||
			//        pmemobj_tx_stage() == TX_STAGE_WORK);
			transaction::manual tx(pop);

			interim_level = make_persistent<level>();
			interim_level->capacity = pow(2, level_size + 1);
			interim_level->buckets = make_persistent<bucket[]>(interim_level->capacity);
			interim_level->mutexes =
				make_persistent<persistent_ptr<mutex_t[assoc_num]>[]>(interim_level->capacity);

			for (difference_type idx = 0;
			     idx < (difference_type)interim_level->capacity; idx++)
				interim_level->mutexes[idx] = make_persistent<mutex_t[assoc_num]>();

			transaction::commit();
		}

		for (difference_type old_idx = 0; old_idx < static_cast<difference_type>(levels[1]->capacity); old_idx++)
		{
			for (size_type i = 0; i < assoc_num; i++)
			{
				if (levels[1]->buckets[old_idx].tokens[i] == 1)
				{
					const key_type &key = levels[1]->buckets[old_idx].slots[i].get_address(my_pool_uuid)->first;

					difference_type f_idx = first_index(first_hash(key), interim_level->capacity);
					difference_type s_idx = second_index(second_hash(key), interim_level->capacity);

					bool succ = false;
					for (size_type j = 0; j < assoc_num; j++)
					{
						if (interim_level->buckets[f_idx].tokens[j] == 0)
						{
							insert_empty_slot(pop, 2, f_idx, j, levels[1]->buckets[old_idx].slots[i]);
							succ = true;
							break;
						}

						if (interim_level->buckets[s_idx].tokens[j] == 0)
						{
							insert_empty_slot(pop, 2, s_idx, j, levels[1]->buckets[old_idx].slots[i]);
							succ = true;
							break;
						}
					}

					if (!succ)
					{
						std::cerr << "The expanding fails: 3" << std::endl;
						exit(1);
					}

					levels[1]->buckets[old_idx].tokens[i] = 0;
					pop.persist(&levels[1]->buckets[old_idx].tokens[i], sizeof(uint8_t));
				}
			}
		}

		{
			transaction::manual tx(pop);
			levels[1]->clear();
			levels[1] = levels[0];
			levels[0] = interim_level;
			interim_level = nullptr;

			level_size = level_size + 1;
			pop.persist(&level_size, sizeof(level_size));

			need_resizing.store(false);
			transaction::commit();
		}

		std::cout << "Expansion completes" << std::endl;
	}

	// Corresponding to "level_destroy" in C version
	void
	clear()
	{
		std::cout << "level destroy!" << std::endl;
	}

	~level_hash()
	{
		clear();
	}

	void foo()
	{
		std::cout << "level_hash::foo()" << std::endl;
	}


// private:
	/**
	 * Get the persistent memory pool where hashmap resides.
	 * @returns pmem::obj::pool_base object.
	 */
	pool_base
	get_pool_base()
	{
		PMEMobjpool *pop =
			pmemobj_pool_by_oid(PMEMoid{my_pool_uuid, 0});

		return pool_base(pop);
	}

	void
	insert_empty_slot(pool_base &pop, size_type level_idx,
		difference_type bucket_idx, size_type slot_idx, kv_ptr_t e)
	{
		bucket *b;
		if (level_idx < 2)
		{
			b = &levels[level_idx]->buckets[bucket_idx];
		}
		else
		{
			b = &interim_level->buckets[bucket_idx];
		}
		assert(b->tokens[slot_idx] == 0);

		b->slots[slot_idx] = e;
		pop.persist(&b->slots[slot_idx].off, sizeof(kv_ptr_t));
		b->tokens[slot_idx] = 1;
		pop.persist((&b->tokens[slot_idx]), sizeof(uint8_t));
	}

	bool
	try_movement(pool_base &pop, size_type level_idx,
		difference_type bucket_idx, kv_ptr_t e)
	{
		for (size_type i = 0; i < assoc_num; i++)
		{
			scoped_t src_guard(levels[level_idx]->mutexes[bucket_idx][i], true);
			const key_type &m_key = levels[level_idx]->buckets[bucket_idx]
				.slots[i].get_address(my_pool_uuid)->first;
			hv_type f_hv = first_hash(m_key);
			hv_type s_hv = second_hash(m_key);
			difference_type f_idx = first_index(f_hv, levels[level_idx]->capacity);
			difference_type s_idx = second_index(s_hv, levels[level_idx]->capacity);

			difference_type jdx = (f_idx == bucket_idx) ? s_idx : f_idx;

			for (size_type j = 0; j < assoc_num; j++)
			{
				scoped_t dst_guard(levels[level_idx]->mutexes[jdx][j], true);
				if (levels[level_idx]->buckets[jdx].tokens[j] == 0)
				{
					insert_empty_slot(pop, level_idx, jdx, j,
						levels[level_idx]->buckets[bucket_idx].slots[i]);

					levels[level_idx]->buckets[bucket_idx].tokens[i] = 0;
					pop.persist(&levels[level_idx]->buckets[bucket_idx].tokens[i], sizeof(uint8_t));

					insert_empty_slot(pop, level_idx, bucket_idx, i, e);

					return true;
				}
			}
		}

		return false;
	}

	bool
	b2t_movement(pool_base &pop, difference_type bucket_idx, kv_ptr_t e)
	{
		for (size_type i = 0; i < assoc_num; i++)
		{
			scoped_t src_guard(levels[1]->mutexes[bucket_idx][i], true);
			assert(levels[1]->buckets[bucket_idx].tokens[i] == 0);

			const key_type &m_key = levels[1]->buckets[bucket_idx].slots[i]
				.get_address(my_pool_uuid)->first;
			hv_type f_hv = first_hash(m_key);
			hv_type s_hv = second_hash(m_key);
			difference_type f_idx = first_index(f_hv, levels[0]->capacity);
			difference_type s_idx = second_index(s_hv, levels[0]->capacity);

			for (size_type j = 0; j < assoc_num; j++)
			{
				scoped_t dst_guard(levels[0]->mutexes[f_idx][j], true);
				if (levels[0]->buckets[f_idx].tokens[j] == 0)
				{
					insert_empty_slot(pop, 0, f_idx, j,
						levels[0]->buckets[f_idx].slots[j]);

					levels[1]->buckets[bucket_idx].tokens[i] = 0;
					pop.persist(&levels[1]->buckets[bucket_idx].tokens[i], sizeof(uint8_t));

					insert_empty_slot(pop, 1, bucket_idx, i, e);

					return true;
				}

				dst_guard.release();
				dst_guard.acquire(levels[0]->mutexes[s_idx][j]);

				if (levels[0]->buckets[s_idx].tokens[j] == 0)
				{
					insert_empty_slot(pop, 0, s_idx, j,
						levels[0]->buckets[s_idx].slots[j]);

					levels[1]->buckets[bucket_idx].tokens[i] = 0;
					pop.persist(&levels[1]->buckets[bucket_idx].tokens[i], sizeof(uint8_t));

					insert_empty_slot(pop, 1, bucket_idx, i, e);

					return true;
				}
			}

		}

		return false;
	}

	uint64_t
	capacity()
	{
		uint64_t n_buckets = levels[0]->capacity;
		n_buckets += levels[1]->capacity;
		if (interim_level)
			n_buckets += interim_level->capacity;

		return n_buckets * assoc_num;
	}


	p<uint64_t> f_seed;
	p<uint64_t> s_seed;
	p<size_type> level_size;
	p<size_type> level_expand_time;
	p<size_type> num_threads;

	persistent_ptr<level> levels[2];
	persistent_ptr<level> interim_level;

	std::atomic<bool> need_resizing;
	barrier resize_barrier; // volatile barrier

	/** ID of persistent memory pool where hash map resides. */
	p<uint64_t> my_pool_uuid;
};

} /* namespace experimental */
} /* namespace obj */
} /* namespace pmem */

#endif /* PMEMOBJ_LEVEL_HASH_HPP */
