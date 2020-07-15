#ifndef PMEMOBJ_CLEVEL_HASH_HPP
#define PMEMOBJ_CLEVEL_HASH_HPP

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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <thread>
#include <time.h>
#include <type_traits>
#include <vector>
#include <sstream>
#include <fstream>
#include <immintrin.h>

#if _MSC_VER
#include <intrin.h>
#include <windows.h>
#endif

#define MAX_LEVEL 16

// #define CLEVEL_DEBUG 1

/**
 * The builtin performs an atomic compare and swap. That is, if the
 * current value of *ptr is oldval, then write newval into *ptr.
 * Return true if the comparison is successful and newval was written.
 */
#define CAS(ptr, oldval, newval) \
	(__sync_bool_compare_and_swap(ptr, oldval, newval))

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

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
	  typename KeyEqual = std::equal_to<Key>, size_t HashPower = 14>
class clevel_hash {
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

	using hv_type = size_t;
	using partial_t = uint16_t;

	typedef enum FindCode
	{
		ABSENT_AND_NO_VACANCY = 0,
		FOUND_IN_LEFT = 1,
		FOUND_IN_RIGHT = 2,
		VACANCY_IN_LEFT = 3,
		VACANCY_IN_RIGHT = 4,
	} f_code_t;

	struct level_bucket;
	struct level_meta;


	using KV_entry_ptr_t = detail::compound_pool_ptr<value_type>;

	using level_ptr_t = detail::compound_pool_ptr<level_bucket>;

	using level_meta_ptr_t = detail::compound_pool_ptr<level_meta>;

#if LIBPMEMOBJ_CPP_USE_TBB_RW_MUTEX
	using mutex_t = pmem::obj::experimental::v<tbb::spin_rw_mutex>;
	using scoped_t = tbb::spin_rw_mutex::scoped_lock;
#else
	using mutex_t = pmem::obj::shared_mutex;
	using scoped_t = shared_mutex_scoped_lock;
#endif

	constexpr static size_type assoc_num = 8;
	constexpr static size_type resize_bulk = 1;

	constexpr static size_type partial_ext_bits
		= (sizeof(uint64_t) - sizeof(partial_t)) * 8;


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
	second_index(partial_t partial, difference_type idx,
		size_type capacity) const
	{
		partial_t nonzero_tag = (partial >> 1 << 1) + 1;
    	// 0xc6a4a7935bd1e995 is the hash constant from 64-bit MurmurHash2
    	uint64_t hash_of_tag = (uint64_t)(nonzero_tag * 0xc6a4a7935bd1e995);
		return static_cast<difference_type>(
			(static_cast<uint64_t>(idx) ^ hash_of_tag) %
			(capacity / 2) + capacity / 2);
	}

	difference_type
	alt_index(partial_t partial, difference_type idx, size_type capacity) const
	{
		partial_t nonzero_tag = (partial >> 1 << 1) + 1;
		// 0xc6a4a7935bd1e995 is the hash constant from 64-bit MurmurHash2
		uint64_t hash_of_tag = (uint64_t)(nonzero_tag * 0xc6a4a7935bd1e995);
		if (static_cast<size_type>(idx) < (capacity / 2))
		{
			return static_cast<difference_type>(
				(static_cast<uint64_t>(idx) ^ hash_of_tag) %
				(capacity / 2) + capacity / 2);
		}
		else
		{
			return static_cast<difference_type>(
				(static_cast<uint64_t>(idx) ^ hash_of_tag) %
				(capacity / 2));
		}
	}

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

	union KV_entry_ptr_u
	{
		KV_entry_ptr_t p;
		struct {
			char padding[6];
			partial_t partial;
		}x;

		KV_entry_ptr_u() : p(nullptr)
		{
		}

		KV_entry_ptr_u(uint64_t off) : p(off)
		{
		}
	};

	struct bucket
	{
		KV_entry_ptr_u slots[assoc_num];
	};

	struct level_bucket
	{
		persistent_ptr<bucket[]> buckets;
		p<uint64_t> capacity;
		level_ptr_t up;

		void
		clear()
		{
			if (buckets)
			{
				delete_persistent<bucket[]>(buckets, capacity);
				buckets = nullptr;
			}
		}
	};

	struct level_meta
	{
		level_ptr_t first_level;
		level_ptr_t last_level;
		p<bool> is_resizing;

		level_meta()
		{
			first_level = nullptr;
			last_level = nullptr;
			is_resizing = false;
		}

		level_meta(const level_ptr_t &fl, const level_ptr_t &ll, bool flag)
		{
			first_level = fl;
			last_level = ll;
			is_resizing = flag;
		}
	};

	static partial_t
	get_partial(hv_type hv)
	{
		constexpr static size_type shift_bits =
			(sizeof(hv_type) - sizeof(partial_t)) * 8;
		return (partial_t)((uint64_t)hv >> shift_bits);
	}

	clevel_hash() : meta(make_persistent<level_meta>().raw().off),
		thread_num(0)
	{
		std::cout << "clevel_hash constructor: HashPower = "
			<< HashPower << std::endl;

		assert(HashPower > 0);
		hashpower.get_rw() = HashPower;

		std::cout << "hashpower : " << hashpower << std::endl;

		// setup pool
		PMEMoid oid = pmemobj_oid(this);
		assert(!OID_IS_NULL(oid));
		my_pool_uuid = oid.pool_uuid_lo;

		level_meta *m = static_cast<level_meta *>(meta(my_pool_uuid));

		persistent_ptr<level_bucket> tmp = make_persistent<level_bucket>();
		tmp->buckets = make_persistent<bucket[]>(pow(2, hashpower));
		tmp->capacity = pow(2, hashpower);
		tmp->up = nullptr;
		m->first_level.off = tmp.raw().off;

		tmp = make_persistent<level_bucket>();
		tmp->buckets = make_persistent<bucket[]>(pow(2, hashpower - 1));
		tmp->capacity = pow(2, hashpower - 1);
		tmp->up = m->first_level;
		m->last_level.off = tmp.raw().off;

		m->is_resizing = false;

		run_expand_thread.get_rw().store(true);
		expand_bucket = 0;
		expand_thread = std::thread(&clevel_hash::resize, this);

		KV_entry_ptr_t e = get_entry(meta(my_pool_uuid)->first_level, 0, 0);
		if (e != nullptr)
		{
			// never fires.
			get_key(e);
		}
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

	ret
	insert(const value_type &value, size_type thread_id, size_type id)
	{
		return generic_insert(value.first, &value,
			allocate_KV_copy_construct, thread_id, id);
	}

	ret
	insert(value_type &&value, size_type thread_id, size_type id)
	{
		return generic_insert(value.first, &value,
			allocate_KV_move_construct, thread_id, id);
	}


	ret
	generic_insert(const key_type &key, const void *param,
		void (*allocate_KV)(pool_base &, persistent_ptr<value_type> &,
		const void *), size_type thread_id, size_type id);

	// mapped_type
	ret
	search(const key_type &key) const;


	ret
	erase(const key_type &key, size_type thread_id);

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
	generic_update(const key_type &key, const void *param,
		void (*allocate_KV)(pool_base &, persistent_ptr<value_type> &,
		const void *), size_type thread_id);

	void
	clear();

	~clevel_hash()
	{
		run_expand_thread.get_rw().store(false);
		expand_thread.join();
		clear();
	}

	// for debug
	void foo()
	{
		std::cout << "clevel_hash::foo()" << std::endl;
		std::cout << "sizeof(KV_entry_ptr_t) = "
			<< sizeof(KV_entry_ptr_t) << std::endl;
		std::cout << "sizeof(KV_entry_ptr_u) = "
			<< sizeof(KV_entry_ptr_u) << std::endl;
		std::cout << "sizeof(meta) = " << sizeof(meta) << std::endl;
	}

	uint64_t
	capacity() const
	{
		return capacity(meta);
	}

	/**
	 * Get the total capacity (#buckets * assoc_num) of given context.
	 */
	uint64_t
	capacity(level_meta_ptr_t m_copy) const
	{
		level_meta *m = static_cast<level_meta *>(m_copy(my_pool_uuid));

		uint64_t total_slots = 0;
		level_ptr_t li;
		for (li = m->last_level; li != m->first_level;)
		{
			level_bucket *cl = li.get_address(my_pool_uuid);
			total_slots += cl->capacity * assoc_num;
			li = cl->up;
		}
		total_slots += li.get_address(my_pool_uuid)->capacity * assoc_num;

		return total_slots;
	}

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
	set_thread_num(size_type num)
	{
		if (thread_num > 0)
		{
			// Reclaim the memory in persistent buffers allocated in previous
			// round of set_thread_num.
			for (size_type i = 0; i < thread_num; i++)
			{
				difference_type di = static_cast<difference_type>(i);
				if (tmp_level[di] != nullptr)
					delete_persistent<level_meta>(tmp_meta[di]);
				if (tmp_level[di] != nullptr)
					delete_persistent<level_bucket>(tmp_level[di]);
				if (tmp_entry[di] != nullptr)
					delete_persistent<value_type>(tmp_entry[di]);
			}
			delete_persistent<persistent_ptr<level_meta>[]>(
				tmp_meta, thread_num);
			delete_persistent<persistent_ptr<level_bucket>[]>(
				tmp_level, thread_num);
			delete_persistent<persistent_ptr<value_type>[]>(
				tmp_entry, thread_num);
		}

		thread_num = num;

#ifdef CLEVEL_DEBUG
		thread_logs.resize(thread_num);
		for (uint64_t i = 0; i < thread_num; i++)
		{
			if (!thread_logs[i].is_open())
			{
				std::stringstream ss;
				ss << "thread-" << i << ".log";
				thread_logs[i].open(ss.str(), std::fstream::out);
			}
		}
#endif

		// Setup persistent buffers according to the thread_num.
		tmp_meta = make_persistent<persistent_ptr<level_meta>[]>(thread_num);
		tmp_level =
			make_persistent<persistent_ptr<level_bucket>[]>(thread_num);
		tmp_entry = make_persistent<persistent_ptr<value_type>[]>(thread_num);
	}

	// Only for debug use!
	KV_entry_ptr_t&
	get_entry(level_ptr_t level, difference_type idx, uint64_t slot_idx);

	// Only for debug use!
	key_type
	get_key(KV_entry_ptr_t &e);

	void
	del_dup(pool_base &pop, KV_entry_ptr_u *p1, KV_entry_ptr_u *p2,
		KV_entry_ptr_t e1, KV_entry_ptr_t e2);

	f_code_t
	find(pool_base &pop, const key_type &key, partial_t partial,
		size_type &n_levels, KV_entry_ptr_t &old_e, KV_entry_ptr_t **e,
		uint64_t &level_num, difference_type &idx, bool fix_dup,
		size_type thread_id, level_meta_ptr_t &m_copy);

	f_code_t
	find_empty_slot(pool_base &pop, const key_type &key, partial_t partial,
		size_type &n_levels, KV_entry_ptr_t **e,
		uint64_t &level_num, level_meta_ptr_t &m_copy);

	void
	expand(pool_base &pop, size_type thread_id, level_meta_ptr_t m_copy);

	void
	resize();

	level_meta_ptr_t meta;

	p<size_type> hashpower;
	p<size_type> thread_num;
	p<difference_type> expand_bucket;
	p<std::atomic<bool>> run_expand_thread;
	persistent_ptr<persistent_ptr<level_meta>[]> tmp_meta;
	persistent_ptr<persistent_ptr<level_bucket>[]> tmp_level;
	persistent_ptr<persistent_ptr<value_type>[]> tmp_entry;

	std::thread expand_thread;

	/** ID of persistent memory pool where hash map resides. */
	p<uint64_t> my_pool_uuid;

#ifdef CLEVEL_DEBUG
	std::vector<std::fstream> thread_logs;
#endif
};

template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
typename clevel_hash<Key, T, Hash, KeyEqual, HashPower>::ret
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::search(
	const key_type &key) const
{
	hv_type hv = hasher{}(key);
	partial_t partial = get_partial(hv);

	while(true)
	{
		level_meta_ptr_t m_copy(meta);
		level_meta *m = static_cast<level_meta *>(m_copy(my_pool_uuid));

		// Bottom-to-top search.
		difference_type f_idx, s_idx;
		size_type i = 0;
		level_ptr_t li = nullptr, next_li = m->last_level;
		do
		{
			li = next_li;
			level_bucket *cl = li.get_address(my_pool_uuid);
			f_idx = first_index(hv, cl->capacity);
			s_idx = second_index(partial, f_idx, cl->capacity);

			bucket &f_b = cl->buckets[f_idx];
			for (size_type j = 0; j < assoc_num; j++)
			{
				if (f_b.slots[j].x.partial == partial
					&& f_b.slots[j].p.get_offset() != 0)
				{
					if (key_equal{}(
						f_b.slots[j].p.get_address(my_pool_uuid)->first, key))
					{
						return ret(i, f_idx, j);
					}
				}
			}

			bucket &s_b = cl->buckets[s_idx];
			for (size_type j = 0; j < assoc_num; j++)
			{
				if (s_b.slots[j].x.partial == partial
					&& s_b.slots[j].p.get_offset() != 0)
				{
					if (key_equal{}(
						s_b.slots[j].p.get_address(my_pool_uuid)->first, key))
					{
						return ret(i, s_idx, j);
					}
				}
			}

			next_li = cl->up;
			i++;
		}while(li != m->first_level);

		// Context checking.
		if (m_copy == meta)
			return ret();
	} // end while(true)
}

template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
void
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::del_dup(
	pool_base &pop, KV_entry_ptr_u *p1, KV_entry_ptr_u *p2,
	KV_entry_ptr_t e1, KV_entry_ptr_t e2)
{
	KV_entry_ptr_u tmp1_u, tmp2_u;
	tmp1_u.p = e1;
	tmp2_u.p = e2;

	if (e1 != p1->p || e2 != p2->p)
		return;

	if (tmp1_u.x.partial == tmp2_u.x.partial)
	{
		// 1. Refer to the same location
		if (e1.get_offset() == e2.get_offset())
		{
			if (CAS(&(p2->p.off), e2.raw(), 0))
			{
				pop.persist(&(p2->p.off), sizeof(uint64_t));
			}
		}

		// 2. Refer to different locations with the same contents
		else if (key_equal{}(e1.get_address(my_pool_uuid)->first,
			e1.get_address(my_pool_uuid)->first))
		{
			if (CAS(&(p2->p.off), e2.raw(), 0))
			{
				pop.persist(&(p2->p.off), sizeof(uint64_t));

				PMEMoid oid = e2.raw_ptr(my_pool_uuid);
				pmemobj_free(&oid);
			}
		}
	}
}

template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
typename clevel_hash<Key, T, Hash, KeyEqual, HashPower>::f_code_t
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::find_empty_slot(
	pool_base &pop, const key_type &key, partial_t partial,
	size_type &n_levels, KV_entry_ptr_t **e,
	uint64_t &level_num, level_meta_ptr_t &m_copy)
{
	hv_type hv = hasher{}(key);
	while (true)
	{
		level_meta *m = static_cast<level_meta *>(m_copy(my_pool_uuid));
		*e = nullptr;

		level_ptr_t levels[MAX_LEVEL];
		difference_type f_idx, s_idx;
		uint64_t slot_idx;

		f_code_t result;

		n_levels = 0;
		level_ptr_t li = nullptr, next_li = m->last_level;
		do
		{
			li = next_li;
			levels[n_levels] = li;
			n_levels++;
			next_li = li.get_address(my_pool_uuid)->up;
		} while(li != m->first_level);

		level_bucket *cl;
		result = ABSENT_AND_NO_VACANCY;

		for (size_type i = n_levels - 1; i < n_levels; i--)
		{
			cl = levels[i].get_address(my_pool_uuid);
			f_idx = first_index(hv, cl->capacity);
			s_idx = second_index(partial, f_idx, cl->capacity);

			// flag used to skip vacant slots after finding an empty
			// slot in a bucket.
			bool found_empty_in_b = false;
			bucket &f_b = cl->buckets[f_idx];
			for (size_type j = 0; j < assoc_num; j++)
			{
				if (!found_empty_in_b && f_b.slots[j].p.get_offset() == 0)
				{
					found_empty_in_b = true;

					result = VACANCY_IN_LEFT;
					*e = &(f_b.slots[j].p);
					level_num = i;
					slot_idx = j;
				}
			}

			// flag used to skip vacant slots after finding an empty
			// slot in a bucket.
			found_empty_in_b = false;
			bucket &s_b = cl->buckets[s_idx];
			for (size_type j = 0; j < assoc_num; j++)
			{
				if (!found_empty_in_b && s_b.slots[j].p.get_offset() == 0)
				{
					found_empty_in_b = true;

					// We prefer the less loaded bucket
					if (result == VACANCY_IN_LEFT && level_num == i &&
						slot_idx <= j)
						continue;

					result = VACANCY_IN_RIGHT;
					*e = &(s_b.slots[j].p);
					level_num = i;
					slot_idx = j;
				}
			}

			if (result != ABSENT_AND_NO_VACANCY)
				break;
		}

		// Context checking.
		if (m_copy == meta)
		{
			return result;
		}
		else
		{
			m_copy = meta;
			pop.persist(&(meta.off), sizeof(uint64_t));
		}
	}
}

template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
typename clevel_hash<Key, T, Hash, KeyEqual, HashPower>::f_code_t
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::find(
	pool_base &pop, const key_type &key, partial_t partial,
	size_type &n_levels, KV_entry_ptr_t &old_e, KV_entry_ptr_t **e,
	uint64_t &level_num, difference_type &idx, bool fix_dup,
	size_type thread_id, level_meta_ptr_t &m_copy)
{
	hv_type hv = hasher{}(key);

	while (true)
	{
RETRY_FIND:
		level_meta *m = static_cast<level_meta *>(m_copy(my_pool_uuid));
		*e = nullptr;

		level_ptr_t levels[MAX_LEVEL];
		difference_type f_idx, s_idx;
		KV_entry_ptr_t f_e, s_e;
		uint64_t slot_idx;

		f_code_t result;
		KV_entry_ptr_t prev_e;
		size_type prev_i;

		n_levels = 0;
		level_ptr_t li = nullptr, next_li = m->last_level;
		do
		{
			li = next_li;
			levels[n_levels] = li;
			n_levels++;
			next_li = li.get_address(my_pool_uuid)->up;
		} while(li != m->first_level);

		level_bucket *cl;
		result = ABSENT_AND_NO_VACANCY;

		// Bottom-to-top search.
		for (size_type i = 0; i < n_levels; i++)
		{
			cl = levels[i].get_address(my_pool_uuid);
			f_idx = first_index(hv, cl->capacity);
			s_idx = second_index(partial, f_idx, cl->capacity);

			// flag used to skip vacant slots after finding an empty slot
			// in a bucket.
			bool found_empty_in_b = false;
			bucket &f_b = cl->buckets[f_idx];
			for (size_type j = 0; j < assoc_num; j++)
			{
				f_e = f_b.slots[j].p;
				if (f_e.get_offset() == 0)
				{
					// Since empty slots in top levels are preferred, update
					// vacancy info as long as identical keys are not found.
					if (result != FOUND_IN_LEFT && result != FOUND_IN_RIGHT &&
						!found_empty_in_b)
					{
						found_empty_in_b = true;

						result = VACANCY_IN_LEFT;
						old_e = f_e;
						*e = &(f_b.slots[j].p);
						level_num = i;
						idx = f_idx;
						slot_idx = j;
					}
					continue;
				}

				if (f_b.slots[j].x.partial != partial || !key_equal{}(
					f_e.get_address(my_pool_uuid)->first, key))
					continue;

				if (!fix_dup)
				{
					result = FOUND_IN_LEFT;
					level_num = i;
					idx = f_idx;
					slot_idx = j;

					return result;
				}

				if (result == FOUND_IN_LEFT || result == FOUND_IN_RIGHT)
				{
					// Refer to the same location
					if (f_e.get_offset() == prev_e.get_offset())
					{
						// Duplication due to the re-insertion in normal
						// executions or os scheduling during a rehashing
						// operation. In this case, delete the pointer in
						// bottom level.
						if (prev_i < i)
						{
							del_dup(pop, &f_b.slots[j], &(levels[level_num]
								.get_address(my_pool_uuid)->buckets[idx]
								.slots[slot_idx]), f_e, prev_e);
						}
						else
						{
							// Never fires!
							assert(false);
						}
					}
					// Refer to different locations
					else
					{
						// Duplication due to the re-insertion after a crash
						// or concurrent insertions of same key. To fix the
						// duplication, simply delete the previous item.
						del_dup(pop, &f_b.slots[j], &(levels[level_num]
							.get_address(my_pool_uuid)->buckets[idx]
							.slots[slot_idx]), f_e, prev_e);
					}
					goto RETRY_FIND;
				}
				else
				{
					result = FOUND_IN_LEFT;
					old_e = f_e;
					*e = &(f_b.slots[j].p);
					level_num = i;
					idx = f_idx;
					slot_idx = j;

					prev_e = f_e;
					prev_i = i;
				} // end if result in FOUND_IN_LEFT or FOUND_IN_RIGHT
			} // end for j, f_idx, f_b

			found_empty_in_b = false;
			bucket &s_b = cl->buckets[s_idx];
			for (size_type j = 0; j < assoc_num; j++)
			{
				s_e = s_b.slots[j].p;
				if (s_e.get_offset() == 0)
				{
					// Since empty slots in top levels are preferred, update
					// vacancy info as long as identical keys are not found.
					if (result != FOUND_IN_LEFT && result != FOUND_IN_RIGHT &&
						!found_empty_in_b)
					{
						found_empty_in_b = true;

						// We prefer the less loaded bucket
						if (result == VACANCY_IN_LEFT && level_num == i &&
							slot_idx <= j)
							continue;

						result = VACANCY_IN_RIGHT;
						old_e = s_e;
						*e = &(s_b.slots[j].p);
						level_num = i;
						idx = s_idx;
						slot_idx = j;
					}
					continue;
				}

				if (s_b.slots[j].x.partial != partial || !key_equal{}(
					s_e.get_address(my_pool_uuid)->first, key))
					continue;

				if (!fix_dup)
				{
					result = FOUND_IN_RIGHT;
					level_num = i;
					idx = s_idx;
					slot_idx = j;

					return result;
				}

				if (result == FOUND_IN_LEFT || result == FOUND_IN_RIGHT)
				{
					// Refer to the same location
					if (s_e.get_offset() == prev_e.get_offset())
					{
						// Duplication due to the re-insertion in normal
						// executions or os scheduling during a rehashing
						// operation. In this case, delete the pointer in
						// bottom level.
						if (prev_i < i)
						{
							del_dup(pop, &s_b.slots[j], &(levels[level_num]
								.get_address(my_pool_uuid)->buckets[idx]
								.slots[slot_idx]), s_e, prev_e);
						}
						else
						{
							// Never fires!
							assert(false);
						}
					}
					// Refer to different locations
					else
					{
						// Duplication due to the re-insertion after a crash
						// or concurrent insertions of same key. To fix the
						// duplication, simply delete the previous item.
						del_dup(pop, &s_b.slots[j], &(levels[level_num]
							.get_address(my_pool_uuid)->buckets[idx]
							.slots[slot_idx]), s_e, prev_e);
					}
					goto RETRY_FIND;
				}
				else
				{
					result = FOUND_IN_RIGHT;
					old_e = s_e;
					*e = &(s_b.slots[j].p);
					level_num = i;
					idx = s_idx;
					slot_idx = j;

					prev_e = s_e;
					prev_i = i;
				} // end if result in FOUND_IN_LEFT or FOUND_IN_RIGHT
			} // end for j, s_idx, s_b

		} // end for i, n_levels; end for first round

		// Context checking.
		if (m_copy == meta)
		{
			return result;
		}
		else
		{
			m_copy = meta;
			pop.persist(&(meta.off), sizeof(uint64_t));
		}
	} // end while
}

template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
typename clevel_hash<Key, T, Hash, KeyEqual, HashPower>::ret
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::generic_insert(
	const key_type &key, const void *param,
	void (*allocate_KV)(pool_base &, persistent_ptr<value_type> &,
		const void *),
	size_type thread_id, size_type id)
{
	pool_base pop = get_pool_base();

	hv_type hv = hasher{}(key);
	partial_t partial = get_partial(hv);

	difference_type t_id = static_cast<difference_type>(thread_id);
	allocate_KV(pop, tmp_entry[t_id], param);
	KV_entry_ptr_u created(tmp_entry[t_id].raw().off);
	created.x.partial = partial;

	bool expanded_flag = false;
	uint64_t initial_capacity = 0;
	bool check_duplicate = true;

#ifdef CLEVEL_DEBUG
	uint64_t retry_insert_cnt = 0;
	thread_logs[thread_id] << "Thread-" << thread_id << " starts inserting "
		<< key << std::endl;
#endif

#ifdef DEBUG_RESIZING
	initial_capacity = capacity();
#endif

	while (true)
	{
RETRY_INSERT:
#ifdef CLEVEL_DEBUG
        retry_insert_cnt ++;
        if (retry_insert_cnt > 10)
        {
            thread_logs[thread_id] << "Thread-" << thread_id
				<< " [loop] retry_insert_cnt = " << retry_insert_cnt
				<< ", key = " << key << std::endl;
        }
        else if (retry_insert_cnt > 1)
        {
            thread_logs[thread_id] << "Thread-" << thread_id
				<< " retry_insert_cnt = " << retry_insert_cnt
				<< ", key = " << key << std::endl;
        }
#endif
		level_meta_ptr_t m_copy(meta);
		pop.persist(&(meta.off), sizeof(uint64_t));

		size_type n_levels;
		uint64_t level_num = 0;
		difference_type idx;
		KV_entry_ptr_t *e, old_e;
		f_code_t result;
		if (check_duplicate)
		{
			result = find(pop, key, partial, n_levels,
				old_e, &e, level_num, idx, /*fix_dup=*/false, thread_id, m_copy);
		}
		else
		{
			result = find_empty_slot(pop, key, partial, n_levels,
				&e, level_num, m_copy);
		}


		level_meta *m = static_cast<level_meta *>(m_copy(my_pool_uuid));

		if (result == FOUND_IN_LEFT || result == FOUND_IN_RIGHT)
		{
			delete_persistent_atomic<value_type>(tmp_entry[t_id]);
			return ret(level_num, 0, 0);
		}
		else if ((result == VACANCY_IN_LEFT || result == VACANCY_IN_RIGHT) &&
			(level_num > 0 || !m->is_resizing))
		{
			if (CAS(&(e->off), old_e.raw(), created.p.raw()))
			{
				if (!m->is_resizing && meta(my_pool_uuid)->is_resizing &&
					level_num == 0)
				{
					// Resizing may occur during the insert. Hence, redo the
					// insertion to avoid missing the new item. The possible
					// duplication will be fixed in future updates and deletes.
					pop.persist(&(meta.off), sizeof(uint64_t));
					check_duplicate = false;
					goto RETRY_INSERT;
				}
				else
				{
					pop.persist(&(e->off), sizeof(uint64_t));

					return ret(expanded_flag, initial_capacity);
				}

			}
			else
			{
#ifdef CLEVEL_DEBUG
				std::cout << "insertion, cas fails, n_levels: " << n_levels
					  << std::endl;
#endif
				goto RETRY_INSERT;
			}
		}

		// start expanding
		expanded_flag = true;
		expand(pop, thread_id, m_copy);
	} // end while(true)
}

template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
typename clevel_hash<Key, T, Hash, KeyEqual, HashPower>::ret
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::erase(
	const key_type &key, size_type thread_id)
{
	pool_base pop = get_pool_base();

	hv_type hv = hasher{}(key);
	partial_t partial = get_partial(hv);
	difference_type expand_bucket_old;
	bool succ_deletion = false;

	while(true)
	{
		level_meta_ptr_t m_copy(meta);
		level_meta *m = static_cast<level_meta *>(m_copy(my_pool_uuid));

		difference_type f_idx, s_idx;
		size_type i = 0;
		level_ptr_t li = nullptr, next_li = m->last_level;
		do
		{
			li = next_li;
			level_bucket *cl = li.get_address(my_pool_uuid);
			f_idx = first_index(hv, cl->capacity);
			s_idx = second_index(partial, f_idx, cl->capacity);

			bucket &f_b = cl->buckets[f_idx];
			for (size_type j = 0; j < assoc_num; j++)
			{
				KV_entry_ptr_u tmp(f_b.slots[j].p.off);
				if (tmp.x.partial == partial
					&& tmp.p.get_offset() != 0)
				{
					if (key_equal{}(
						tmp.p.get_address(my_pool_uuid)->first, key))
					{
						if (CAS(&(f_b.slots[j].p.off), tmp.p.off, 0))
						{
							pop.persist(&(f_b.slots[j].p.off), sizeof(uint64_t));
							succ_deletion = true;

							PMEMoid oid = tmp.p.raw_ptr(my_pool_uuid);
							pmemobj_free(&oid);


				// Instead of redoing the delete to guarantee the deletion is
				// successful, we apply context checking to avoid unnecessary
				// re-executions. The deletion fails only when the item to be
				// deleted is copied by rehashing threads after checking and
				// before deletion's CAS. Therefore, we can do context
				// checking to avoid such failures.
							if (m_copy != meta || (i == 0
								&& f_idx <= expand_bucket
								&& f_idx >= expand_bucket_old))
							{
								continue;
							}
						}
						else
						{
							continue;
						}
					}
				}
			}

			bucket &s_b = cl->buckets[s_idx];
			for (size_type j = 0; j < assoc_num; j++)
			{
				KV_entry_ptr_u tmp(s_b.slots[j].p.off);
				if (tmp.x.partial == partial
					&& tmp.p.get_offset() != 0)
				{
					if (key_equal{}(
						tmp.p.get_address(my_pool_uuid)->first, key))
					{
						if (CAS(&(s_b.slots[j].p.off), tmp.p.off, 0))
						{
							pop.persist(&(s_b.slots[j].p.off), sizeof(uint64_t));
							succ_deletion = true;

							PMEMoid oid = tmp.p.raw_ptr(my_pool_uuid);
							pmemobj_free(&oid);


				// Instead of redoing the delete to guarantee the deletion is
				// successful, we apply context checking to avoid unnecessary
				// re-executions. The deletion fails only when the item to be
				// deleted is copied by rehashing threads after checking and
				// before deletion's CAS. Therefore, we can do context
				// checking to avoid such failures.
							if (m_copy != meta || (i == 0
								&& s_idx <= expand_bucket
								&& s_idx >= expand_bucket_old))
							{
								continue;
							}
						}
						else
						{
							continue;
						}
					}
				}
			}
			next_li = cl->up;
			i++;
		}while(li != m->first_level);

		// Context checking.
		if (m_copy == meta)
			return ret(succ_deletion);
	} // end while(true)

}

template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
typename clevel_hash<Key, T, Hash, KeyEqual, HashPower>::ret
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::generic_update(
	const key_type &key, const void *param,
	void (*allocate_KV)(pool_base&, persistent_ptr<value_type>&, const void*),
	size_type thread_id)
{
	pool_base pop = get_pool_base();

	hv_type hv = hasher{}(key);
	partial_t partial = get_partial(hv);

	difference_type t_id = static_cast<difference_type>(thread_id);
	allocate_KV(pop, tmp_entry[t_id], param);
	KV_entry_ptr_u created(tmp_entry[t_id].raw().off);
	created.x.partial = partial;

	difference_type expand_bucket_old;
	bool succ_update = false;
	while (true)
	{
		level_meta_ptr_t m_copy(meta);
		pop.persist(&(meta.off), sizeof(uint64_t));

		size_type n_levels;
		uint64_t level_num = 0;
		difference_type idx;
		KV_entry_ptr_t *e, old_e;

		expand_bucket_old = expand_bucket;
		f_code_t result = find(pop, key, partial, n_levels,
			old_e, &e, level_num, idx, /*fix_dup=*/true, thread_id, m_copy);

		if (result == FOUND_IN_LEFT || result == FOUND_IN_RIGHT)
		{
			if (succ_update && old_e == created.p)
			{
				// The only item in table after update is the modified one,
				// which indicates a successful update.
				return ret(true);
			}
			else if (CAS(&(e->off), old_e.raw(), created.p.raw()))
			{
				pop.persist(&(e->off), sizeof(uint64_t));

				// Instead of simply issuing another find to guarantee the
				// update is successful, we apply context checking to avoid
				// unnecessary second find. The update fails only when the
				// item to be updated is copied by rehashing threads after
				// find and before update's CAS. Therefore, we can do
				// context checking to avoid such failure.
				if (m_copy != meta || (level_num == 0 && idx <= expand_bucket
					&& idx >= expand_bucket_old))
				{
					succ_update = true;
					continue;
				}
				else
					return ret(true);
			}
		}
		else
		{
			if (!succ_update)
			{
				delete_persistent_atomic<value_type>(tmp_entry[t_id]);
			}
			// Even the updated item is deleted by other threads, our update
			// succeeds anyway.
			return ret(succ_update);
		}
	}
}

template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
void
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::expand(
	pool_base &pop, size_type thread_id, level_meta_ptr_t m_copy
)
{
	level_meta *m = static_cast<level_meta *>(m_copy(my_pool_uuid));
	difference_type t_id = static_cast<difference_type>(thread_id);
	level_bucket *cl = m->first_level.get_address(my_pool_uuid);

	if (cl->up == nullptr)
	{
		make_persistent_atomic<level_bucket>(pop, tmp_level[t_id]);
		size_type new_capacity = cl->capacity * 2;
		std::cout << "Thread-" << thread_id << " starts expanding for "
			<< new_capacity << " buckets" << std::endl;

		make_persistent_atomic<bucket[]>(
			pop, tmp_level[t_id]->buckets, new_capacity);

		pop.persist(tmp_level[t_id]->buckets);
		tmp_level[t_id]->capacity = new_capacity;
		pop.persist(tmp_level[t_id]->capacity);
		tmp_level[t_id]->up = nullptr;
		pop.persist(&(tmp_level[t_id]->up.off), sizeof(uint64_t));

		// Append a new level.
		bool rc = CAS(&(cl->up.off), 0, tmp_level[t_id].raw().off);

		if (rc == false)
		{
			// Ohter threads finished expanding
			pop.persist(&(cl->up.off), sizeof(uint64_t));

			delete_persistent_atomic<bucket[]>(
				tmp_level[t_id]->buckets, new_capacity);

			delete_persistent_atomic<level_bucket>(tmp_level[t_id]);
		}

		pop.persist(&(cl->up.off), sizeof(uint64_t));

		// Update the first_level and is_resizing in the metadata.
		while (true)
		{
			if (cl->capacity >= new_capacity)
			{
				// Help updating meta
				make_persistent_atomic<level_meta>(pop, tmp_meta[t_id],
					m->first_level, m->last_level, true);
			}
			else
			{
				assert(cl->up != nullptr);
				make_persistent_atomic<level_meta>(pop, tmp_meta[t_id],
					cl->up, m->last_level, true);
			}

			if (CAS(&(meta.off), m_copy.off, tmp_meta[t_id].raw().off))
			{
				pop.persist(&(meta.off), sizeof(uint64_t));

				std::cout << "Thread-" << thread_id
					<< " finishes expanding, capacity: "
					<< capacity() << std::endl;
				break;
			}
			else
			{
				m_copy = level_meta_ptr_t(meta);
				m = static_cast<level_meta *>(m_copy(my_pool_uuid));
				cl = m->first_level.get_address(my_pool_uuid);

				if (cl->capacity >= new_capacity && m->is_resizing)
				{
					// CAS fails because other threads help updating meta
					delete_persistent_atomic<level_meta>(tmp_meta[t_id]);
					break;
				}
				// CAS fails because other threads complete rehashing.
			}
		}
	}
	else
	{
		// Ohter threads finished expanding
		pop.persist(&(cl->up.off), sizeof(uint64_t));

		if (meta == m_copy)
		{
			size_type new_capacity = cl->capacity;

			// Update the first_level and is_resizing in the metadata.
			while (true)
			{
				// Help updating meta
				if (cl->capacity >= new_capacity)
				{
					make_persistent_atomic<level_meta>(pop, tmp_meta[t_id],
						m->first_level, m->last_level, true);
				}
				else
				{
					assert(cl->up != nullptr);
					make_persistent_atomic<level_meta>(pop, tmp_meta[t_id],
						cl->up, m->last_level, true);
				}

				if (CAS(&(meta.off), m_copy.off, tmp_meta[t_id].raw().off))
				{
					pop.persist(&(meta.off), sizeof(uint64_t));

					std::cout << "Thread-" << thread_id
						<< " finishes expanding, capacity: "
						<< capacity() << std::endl;
					break;
				}
				else
				{
					m_copy = level_meta_ptr_t(meta);
					m = static_cast<level_meta *>(m_copy(my_pool_uuid));
					cl = m->first_level.get_address(my_pool_uuid);

					if (cl->capacity >= new_capacity && m->is_resizing)
					{
						// CAS fails because other threads help updating meta
						delete_persistent_atomic<level_meta>(tmp_meta[t_id]);
						break;
					}
					// CAS fails because other threads complete rehashing.
				}
			}
		}
	}
}

template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
void
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::resize()
{
	size_type thread_id = 0;
	difference_type t_id = static_cast<difference_type>(thread_id);
	pool_base pop = get_pool_base();

	while (run_expand_thread.get_ro().load())
	{
		level_meta_ptr_t m_copy(meta);
		pop.persist(&(meta.off), sizeof(uint64_t));

		level_meta *m = static_cast<level_meta *>(m_copy(my_pool_uuid));

		size_type n_levels = 1;
		if (m != nullptr)
		{
			for (auto li = m->last_level; li != m->first_level;
			    li = li.get_address(my_pool_uuid)->up)
				n_levels++;
		}

		if (m == nullptr || n_levels == 2)
		{
			usleep(10000);
			continue;
		}

		for (size_type ii = 0; ii < resize_bulk; ii++)
		{
RETRY_REHASH:
			m_copy = level_meta_ptr_t(meta);
			pop.persist(&(meta.off), sizeof(uint64_t));

			m = static_cast<level_meta *>(m_copy(my_pool_uuid));
			level_bucket *bl = m->last_level.get_address(my_pool_uuid);
			level_bucket *tl = m->first_level.get_address(my_pool_uuid);

			bucket &b = bl->buckets[expand_bucket.get_ro()];
			for (size_type slot_idx = 0; slot_idx < assoc_num; slot_idx++)
			{
				KV_entry_ptr_t src_tmp = b.slots[slot_idx].p;
				value_type *e = src_tmp.get_address(my_pool_uuid);
				if (e == nullptr)
					continue;

				difference_type f_idx, s_idx;
				bool succ = false;
				hv_type hv = hasher{}(e->first);
				partial_t partial = get_partial(hv);
				f_idx = first_index(hv, tl->capacity);
				s_idx = second_index(partial, f_idx, tl->capacity);

				bucket &dst_b1 = tl->buckets[f_idx];
				bucket &dst_b2 = tl->buckets[s_idx];
				for (size_type j = 0; j < assoc_num; j++)
				{
					// The rehashed item is inserted into the less-loaded
					// bucket between the two candidata buckets in the new
					// level.
					KV_entry_ptr_t dst_tmp = dst_b1.slots[j].p;
					if (dst_tmp.get_offset() == 0)
					{
						if (CAS(&(dst_b1.slots[j].p.off),
							dst_tmp.raw(), src_tmp.raw()))
						{
							pop.persist(&(dst_b1.slots[j].p.off),
								sizeof(uint64_t));

							b.slots[slot_idx].p = nullptr;
							pop.persist(&(b.slots[slot_idx].p.off),
								sizeof(uint64_t));
							succ = true;
							break;
						}
					}

					dst_tmp = dst_b2.slots[j].p;
					if (dst_tmp.get_offset() == 0)
					{
						if (CAS(&(dst_b2.slots[j].p.off),
							dst_tmp.raw(), src_tmp.raw()))
						{
							pop.persist(&(dst_b2.slots[j].p.off),
								sizeof(uint64_t));

							b.slots[slot_idx].p = nullptr;
							pop.persist(&(b.slots[slot_idx].p.off),
								sizeof(uint64_t));
							succ = true;
							break;
						}
					}
				} // end for

				if (!succ)
				{
					std::cout << "expand during resizing!" << std::endl;
					expand(pop, thread_id, m_copy);
					goto RETRY_REHASH;
				}
			} // end for (slot_idx)

			expand_bucket = expand_bucket + 1;
			pop.persist(expand_bucket);
			if (static_cast<size_type>(expand_bucket) == bl->capacity)
			{
				bool rc = false;
				while (true)
				{
					level_ptr_t li = m->last_level;
					size_t levels_left = 0;
					while (li != m->first_level)
					{
						levels_left++;
						li = li.get_address(my_pool_uuid)->up;
					}
					make_persistent_atomic<level_meta>(pop, tmp_meta[t_id],
						m->first_level, bl->up, levels_left != 2);

					if (CAS(&(meta.off), m_copy.off, tmp_meta[t_id].raw().off))
					{
						std::cout << "Expand thread updates metadata, "
							<< "is_resizing: " << bool(levels_left != 2)
							<<  " levels_left: " << levels_left
							<< std::endl;
						pop.persist(&(meta.off), sizeof(uint64_t));

						expand_bucket.get_rw() = 0;
						pop.persist(expand_bucket);

						rc = true;
						break;
					}
					else
					{
						delete_persistent_atomic<level_meta>(tmp_meta[t_id]);
						m_copy = level_meta_ptr_t(meta);
						pop.persist(&(meta.off), sizeof(uint64_t));
						m = static_cast<level_meta *>(m_copy(my_pool_uuid));
					}
				}

				if (rc)
					break;
			}
		} // end for (ii)
	} // end while(run_expand_thread)

	std::cout << "expand_thread exits" << std::endl;
}

template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
typename clevel_hash<Key, T, Hash, KeyEqual, HashPower>::KV_entry_ptr_t&
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::get_entry(
	level_ptr_t level, difference_type idx, uint64_t slot_idx)
{
	return level.get_address(my_pool_uuid)->buckets[idx].slots[slot_idx].p;
}

template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
typename clevel_hash<Key, T, Hash, KeyEqual, HashPower>::key_type
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::get_key(
	KV_entry_ptr_t &e)
{
	return e.get_address(my_pool_uuid)->first;
}


template <typename Key, typename T, typename Hash, typename KeyEqual,
	size_t HashPower>
void
clevel_hash<Key, T, Hash, KeyEqual, HashPower>::clear()
{
	std::cout << "level destroy!" << std::endl;
}

} /* namespace experimental */
} /* namespace obj */
} /* namespace pmem */

#endif /* PMEMOBJ_CLEVEL_HASH_HPP */
