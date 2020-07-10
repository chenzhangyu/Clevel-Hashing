#ifndef PMEMOBJ_CCEH_HPP
#define PMEMOBJ_CCEH_HPP

#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/detail/template_helpers.hpp>
#include <libpmemobj++/detail/compound_pool_ptr.hpp>
#include <libpmemobj++/experimental/concurrent_hash_map.hpp>
#include <libpmemobj++/experimental/hash.hpp>
#include <libpmemobj++/experimental/v.hpp>
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
#include <unordered_map>

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

class CCEH {
public:
	using key_type = uint8_t *;
	using mapped_type = uint8_t *;
	using value_type = std::pair<const key_type, mapped_type>;
	using size_type = size_t;
	using difference_type = ptrdiff_t;
	using pointer = value_type *;
	using const_pointer = const value_type *;
	using reference = value_type &;
	using const_reference = const value_type &;
	using hv_type = size_t;


#if LIBPMEMOBJ_CPP_USE_TBB_RW_MUTEX
	using mutex_t = pmem::obj::experimental::v<tbb::spin_rw_mutex>;
	using scoped_t = tbb::spin_rw_mutex::scoped_lock;
#else
	using mutex_t = pmem::obj::shared_mutex;
	using scoped_t = shared_mutex_scoped_lock;
#endif

    constexpr static size_type kSegmentBits = 8;
    constexpr static size_type kMask = (1 << kSegmentBits)-1;//...1111 1111
    constexpr static size_type kShift = kSegmentBits;
    constexpr static size_type kSegmentSize = (1 << kSegmentBits) * 16 * 4;
	constexpr static size_type kNumPairPerCacheLine = 4;
    constexpr static size_type kNumCacheLine = 4;

    constexpr static size_type KNumSlot = 1024;

/* 
hash calculator
description: 
	The hash function comes from the C++ Standard Template Library(STL).
*/
	class hasher
	{
	public:
		hv_type operator()(const key_type &key, size_type sz)
		{
			return std::hash<std::string>{}(std::string(reinterpret_cast<char *>(key), sz));
		}
	};

// hash comparison
	class key_equal 
	{
	public:
		bool
		operator()(const key_type &lhs, const key_type &rhs, size_type sz) const
		{
			return strncmp(reinterpret_cast<const char *>(lhs), 
				reinterpret_cast<const char *>(rhs), sz) == 0;
		}
	};

	struct ret
	{
		bool found;
		uint8_t segment_idx;
		difference_type bucket_idx;

		ret(size_type _segment_idx, difference_type _bucket_idx)
		    : found(true), segment_idx(_segment_idx), bucket_idx(_bucket_idx)
		{
		}

		ret() : found(false), segment_idx(0), bucket_idx(0)
		{
		}
	};

/* 
data structure of KV pair
description: 
	To support variable-length items,
	our implementation stores pointers to the KV pairs in the hash table,
	instead of storing KV pairs directly,
	which is different from the original CCEH.
*/
    struct KV_entry
    {
        persistent_ptr<uint8_t[]> key;
        persistent_ptr<uint8_t[]> value;
		p<size_type> key_len;
		p<size_type> value_len;

    };
/*
data structure of segment
description:
	The same type of reader/writer lock from cmap is used for
	the segment and slot locks.
*/
	struct segment
	{
		persistent_ptr<KV_entry> slots[KNumSlot];
		mutex_t bucket_lock[KNumSlot];
		mutex_t segment_lock;
		p<size_type> local_depth;
		p<size_type> pattern;

		segment(void):local_depth{0}
		{

		}

		segment(size_type _depth) : local_depth(_depth)
		{
			
		}
		~segment(void)
		{

		}

		size_type segment_insert(const key_type &key, size_type key_len, mapped_type value, 
			size_type value_len, size_t loc, size_t key_hash, pool_base &pop)
		{
			if((key_hash >> (8*sizeof(key_hash)-local_depth)) != pattern) return 2;
			size_type rc=1;
			scoped_t lock_segment(segment_lock, false);//true：write lock，false: read lock
			for(size_type i=0;i<kNumPairPerCacheLine*kNumCacheLine;i++)
			{
				auto slot_idx = (loc + i) % KNumSlot;
				scoped_t lock_bucket(bucket_lock[slot_idx],true);//true：write lock，false: read lock
				if (slots[slot_idx] != nullptr)
				{
					hv_type hv = hasher{}(slots[slot_idx]->key.get(), key_len);
					if ((hv >> (8*sizeof(key_hash)-local_depth)) == pattern)
					{
						continue;
					}
					else
					{
						transaction::run(pop, [&] {
							slots[slot_idx]=make_persistent<KV_entry>();
							slots[slot_idx]->key=make_persistent<uint8_t[]>(key_len);
							slots[slot_idx]->value=make_persistent<uint8_t[]>(value_len);
							slots[slot_idx]->value_len = value_len;
							memcpy(slots[slot_idx]->value.get(), value, value_len);
							slots[slot_idx]->key_len = key_len;
							memcpy(slots[slot_idx]->key.get()+8, key+8, key_len-8);	
							pop.drain();
							memcpy(slots[slot_idx]->key.get(), key, 8);
						});
						rc=0;
						break;
					}
						
				}
				else
				{
					transaction::run(pop, [&] {
					slots[slot_idx]=make_persistent<KV_entry>();
					slots[slot_idx]->key=make_persistent<uint8_t[]>(key_len);
					slots[slot_idx]->value=make_persistent<uint8_t[]>(value_len);
					slots[slot_idx]->value_len = value_len;
					memcpy(slots[slot_idx]->value.get(), value, value_len);
					slots[slot_idx]->key_len = key_len;
					memcpy(slots[slot_idx]->key.get()+8, key+8, key_len-8);	
					pop.drain();
					memcpy(slots[slot_idx]->key.get(), key, 8);
					});
					rc=0;
					break;
				}
			}
			lock_segment.release();
			return rc;
		}
/*
migrate inserted items to the new segment for segment split operation
description:
	For efficient migration, we only copy the pointers of inserted items without allocating memory again.
*/
		void Insert4split(persistent_ptr<KV_entry> & old_slot, size_t loc,pool_base &pop) 
		{
  			for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) 
			{
   			 	auto slot_idx = (loc + i) % KNumSlot;
				if(slots[slot_idx] == nullptr)
				{
					scoped_t lock_bucket(bucket_lock[slot_idx],true);//true：write lock，false: read lock
					transaction::run(pop, [&] {
						slots[slot_idx]=old_slot;
					});
					lock_bucket.release();
					break;
				}	
  			}
			  return;
		}

		persistent_ptr<persistent_ptr<segment>[]> Split(pool_base &pop,hv_type key_hash)
		{
  			using namespace std;
			scoped_t lock_segment;
			if(!lock_segment.try_acquire(segment_lock,true))return nullptr;//try to acquire segment write lock
			persistent_ptr<persistent_ptr<segment>[]> split_segment;
			
			transaction::run(pop, [&] {
 	 		split_segment = make_persistent<persistent_ptr<segment>[]>(2);
  			split_segment[0] = this;
  			split_segment[1] = make_persistent<segment>(local_depth+1);
			});

  			for (size_type i = 0; i < KNumSlot; i++) 
			{
				if(slots[i]!=nullptr)
				{
					hv_type hv = hasher{}(slots[i]->key.get(), slots[i]->key_len);
					if (hv & ((hv_type)1<<(sizeof(hv_type)*8-local_depth-1)))
					{
						split_segment[1]->Insert4split(slots[i],(hv & kMask)*kNumPairPerCacheLine,pop);

					}
				}
  			}			

  			pop.persist((char*)split_segment[1].get(), sizeof(segment));
  			local_depth = local_depth + 1;
  			pop.persist((char*)&local_depth, sizeof(size_t));
			split_segment[0]->pattern=(key_hash>>(8*sizeof(key_hash)-split_segment[0]->local_depth+1))<<1;
			
			lock_segment.release();
			
  			return split_segment;
		}
	};

	struct directory
	{
		static const size_type kDefaultDepth = 10;
		persistent_ptr<persistent_ptr<segment>[]> segments;
		p<size_type> capacity;
		p<size_type> depth;
		mutex_t m;
		
		
		directory(pool_base &pop)
		{
			depth=kDefaultDepth;
			capacity=pow(2,depth);

			transaction::run(pop, [&] {
			segments=make_persistent<persistent_ptr<segment>[]>(capacity);
			});
		}

		directory(size_type _depth,pool_base &pop)
		{
			depth = _depth;
			capacity = pow(2, depth);

			transaction::run(pop, [&] {
			segments = make_persistent<persistent_ptr<segment>[]>(capacity);

			});
		}

		~directory()
		{
			if (segments)
			{
				delete_persistent_atomic<persistent_ptr<segment>[]>(segments, capacity);
			}
		}

	};

	CCEH(size_type _depth) 
	{
		PMEMoid oid = pmemobj_oid(this);

		assert(!OID_IS_NULL(oid));

		my_pool_uuid = oid.pool_uuid_lo;

		pool_base pop= get_pool_base();

		transaction::run(pop, [&] {
		dir = make_persistent<directory>(_depth,pop);
		for(size_type i=0;i<pow(2,_depth);i++)
		{
			dir->segments[i]=make_persistent<segment>(_depth);
			dir->segments[i]->pattern = i;
		}
		});
	}

	ret insert(const key_type &key, const mapped_type &value,
		size_type key_len, size_type value_len, size_type id)
	{
		pool_base pop = get_pool_base();
	STARTOVER:
		hv_type key_hash=hasher{}(key, key_len);
		size_type y=(key_hash&kMask)*kNumPairPerCacheLine;

	RETRY:
		scoped_t lock_dir;//directory_lock
		size_type x=(key_hash>>(8*sizeof(key_hash)-dir->depth));
		persistent_ptr<segment> target=dir->segments[x];
		size_type rc=target->segment_insert(key,key_len,value,value_len,y,key_hash,pop);

		if(rc==1)//insertion failure: hash collision, need segment split
		{
			persistent_ptr<persistent_ptr<segment>[]> sp_segment = target->Split(pop,key_hash);
			
			if(sp_segment.get()==nullptr) 
			{
				goto RETRY;
			}

			sp_segment[1]->pattern=((key_hash>>(8*sizeof(key_hash)-sp_segment[0]->local_depth+1))<<1)+1;

			lock_dir.acquire(dir->m, true);//acquire write lock for directory

			x=(key_hash>>(8*sizeof(key_hash)-dir->depth));
			if((dir->segments[x]->local_depth-1)<dir->depth)//segment split without directory doubling
			{
				size_type depth_diff= dir->depth-sp_segment[0]->local_depth;
				if(depth_diff==0)
				{
					if(x%2==0)
					{
						dir->segments[x+1]=sp_segment[1];
						pop.persist(&dir->segments[x+1],sizeof(persistent_ptr<segment>));
					}
					else
					{
						dir->segments[x]=sp_segment[1];
						pop.persist(&dir->segments[x],sizeof(persistent_ptr<segment>));
					}
				}
				else
				{
					size_type chunk_size=pow(2,dir->depth-(sp_segment[0]->local_depth-1));
					x = x-(x%chunk_size);
					for(size_type i=0;i<chunk_size/2;i++)
					{
						dir->segments[x+chunk_size/2+i]=sp_segment[1];						
					}
					pop.persist(&dir->segments[x+chunk_size/2],sizeof(void*)*chunk_size/2);
				}
				lock_dir.release();	
			}
			else// directory doubling for segment split
			{
				persistent_ptr<directory> dir_old = dir;
				persistent_ptr<persistent_ptr<segment>[]> dir_old_segments=dir->segments;
				transaction::run(pop, [&] {
				persistent_ptr<directory> new_dir = make_persistent<directory>(dir->depth+1,pop);
				
				for(size_type i=0;i<dir->capacity;i++)
				{
					if(i==x)
					{
						new_dir->segments[2*i]   = sp_segment[0];
						new_dir->segments[2*i+1] = sp_segment[1];
					}
					else 
					{
						new_dir->segments[2*i]   = dir_old_segments[i];
						new_dir->segments[2*i+1] = dir_old_segments[i];
					}
					pop.persist(new_dir->segments[2*i]);
					pop.persist(new_dir->segments[2*i+1]);
				}
				printf("directory->depth:%ld\n",new_dir->depth);
				dir = new_dir;
				pop.persist(dir);

				delete_persistent<directory>(dir_old);
				});
				lock_dir.release();
			}
			
			goto RETRY;
		}
		else if(rc==2)// insertion failure: the pattern of the item does not match the pattern of current segment
		{
			goto STARTOVER;
		}
		else// insertion success: do nothing
		{
			
		}
		
		return ret(x, y);
	}

	ret get(const key_type &key, size_type key_len)
	{
		hv_type key_hash=hasher{}(key, key_len);
		size_type y=(key_hash&kMask)*kNumPairPerCacheLine;
		/* The directory reader locks protect readers from accessing a reclaimed directory,
		    which guarantees the thread safety for directory. */
		scoped_t lock_dir(dir->m, false);//directory reader lock
		size_type x=(key_hash>>(8*sizeof(key_hash)-dir->depth));

		persistent_ptr<segment> target_segment = dir->segments[x];
		if(target_segment->local_depth<dir->depth)
		{
			x=(key_hash>>(8*sizeof(key_hash)-target_segment->local_depth));
		}

		scoped_t lock_target_segment(target_segment->segment_lock, false);//true：write lock，false: read lock
		for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i)
		{
			size_type location = (y+i) % KNumSlot;
			if(target_segment->slots[location] != nullptr && target_segment->slots[location]->key.get()!=0x0
				&& key_equal{}(key,target_segment->slots[location]->key.get(),key_len))
			{
				lock_dir.release();
				lock_target_segment.release();
				return ret(x, y);
			}
		}
		lock_dir.release();
		lock_target_segment.release();
		return ret();
	}

	size_t Capacity(void) 
	{
  		std::unordered_map<segment*,bool> set;
  		for (size_t i = 0; i < dir->capacity; ++i) 
		{
    		set[dir->segments[i].get()] = true;
  		}
  		return set.size() * KNumSlot;
	}
	
	void 
	clear()
	{
	}
	
	~CCEH()
	{
		clear();
	}

	void foo()
	{
		std::cout << "CCEH::foo()" << std::endl;
	}


	pool_base
	get_pool_base()
	{
		PMEMobjpool *pop =
			pmemobj_pool_by_oid(PMEMoid{my_pool_uuid, 0});

		return pool_base(pop);
	}

	persistent_ptr<directory> dir;
	p<uint64_t> my_pool_uuid;
};

} /* namespace experimental */
} /* namespace obj */
} /* namespace pmem */

#endif /* PMEMOBJ_CCEH_HPP */
