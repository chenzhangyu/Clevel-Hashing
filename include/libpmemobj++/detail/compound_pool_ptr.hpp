#ifndef LIBPMEMOBJ_CPP_COMPOUND_POOL_PTR_HPP
#define LIBPMEMOBJ_CPP_COMPOUND_POOL_PTR_HPP

namespace pmem
{

namespace detail
{

/**
 * Similar to the "persistent_pool_ptr" except for two differences:
 * (1) no transactions
 * (2) "off" encodes "partial", "address", and "marker" info
 */
template <typename T>
class compound_pool_ptr {
	template <typename Y>
	friend class compound_pool_ptr;

	typedef compound_pool_ptr<T> this_type;

public:
	/**
	 * Type of an actual object with all qualifier removed,
	 * used for easy underlying type access
	 */
	typedef typename pmem::detail::sp_element<T>::type element_type;

	compound_pool_ptr() : off(0)
	{
		verify_type();
	}

	/**
	 *  Default null constructor, zeroes the off.
	 */
	compound_pool_ptr(std::nullptr_t) noexcept : off(0)
	{
		verify_type();
	}

	/**
	 * PMEMoid constructor.
	 *
	 * Provided for easy interoperability between C++ and C API's.
	 *
	 * @param oid C-style persistent pointer
	 */
	compound_pool_ptr(PMEMoid oid) noexcept : off(oid.off)
	{
		verify_type();
	}

	/**
	 * PMEMoid constructor.
	 *
	 * Provided for easy interoperability between C++ and C API's.
	 *
	 * @param off offset inside persistent memory pool
	 */
	compound_pool_ptr(uint64_t _off) noexcept : off(_off)
	{
		verify_type();
	}

	/*
		* Copy constructor.
		*
		* @param r Persistent pool pointer to the same type.
		*/
	compound_pool_ptr(const compound_pool_ptr &r) noexcept
		: off(r.off)
	{
		verify_type();
	}

	/**
	 * Move constructor.
	 */
	compound_pool_ptr(compound_pool_ptr &&r) noexcept
		: off(std::move(r.off))
	{
		verify_type();
	}

	/**
	 * Move assignment operator.
	 */
	compound_pool_ptr &
	operator=(compound_pool_ptr &&r)
	{
		this->off = std::move(r.off);

		return *this;
	}

	compound_pool_ptr &operator=(std::nullptr_t)
	{
		this->off = 0;

		return *this;
	}

	compound_pool_ptr &
	operator=(const compound_pool_ptr &r)
	{
		this->off = r.off;

		return *this;
	}

	const uint64_t &
	raw() const noexcept
	{
		return this->off;
	}

	PMEMoid
	raw_ptr(uint64_t pool_uuid) const noexcept
	{
		uint64_t ptr = (this->off & 0x0000FFFFFFFFFFFC);
		PMEMoid oid = {pool_uuid, ptr};
		return oid;
	}

	/**
	 * Get a direct pointer.
	 *
	 * Performs a calculations on the underlying C-style pointer.
	 *
	 * @return a direct pointer to the object.
	 */
	element_type *
	get_address(uint64_t pool_uuid) const noexcept
	{
		uint64_t ptr = (this->off & 0x0000FFFFFFFFFFFC);
		PMEMoid oid = {pool_uuid, ptr};
		return static_cast<element_type *>(pmemobj_direct(oid));
	}

	element_type *
	operator()(uint64_t pool_uuid) const noexcept
	{
		return get_address(pool_uuid);
	}

	/**
	 * Swaps two compound_pool_ptr objects of the same type.
	 */
	void
	swap(compound_pool_ptr &other) noexcept
	{
		std::swap(this->off, other.off);
	}

	/*
	 * Bool conversion operator.
	 */
	explicit operator bool() const noexcept
	{
		return this->off != 0;
	}

	/**
	 * Get the two-bit marker.
	 */
	uint8_t
	get_marker() const
	{
		return (uint8_t)(this->off & 0x3);
	}

	/**
	 * Advance the two-bit marker.
	 */
	uint64_t
	next_state() const
	{
		return (this->off & 0xFFFFFFFFFFFFFFFC) | (get_marker() + 1U);
	}

	bool
	is_next_state_of(const compound_pool_ptr &r)
	{
		return get_marker() == (r.get_marker() + 1U);
	}

	/**
	 * Get the encoded offset in compound_pool_ptr.
	 */
	uint64_t
	get_offset()
	{
		return (this->off & 0x0000FFFFFFFFFFFC);
	}

// private:
	/* offset of persistent object in a persistent memory pool*/
	uint64_t off;

	void
	verify_type()
	{
		static_assert(!std::is_polymorphic<element_type>::value,
					"Polymorphic types are not supported");
	}
};

/**
 * Equality operator.
 *
 * This checks if underlying PMEMoids are equal.
 */
template <typename T, typename Y>
inline bool
operator==(const compound_pool_ptr<T> &lhs,
	   const compound_pool_ptr<Y> &rhs) noexcept
{
	return lhs.raw() == rhs.raw();
}

/**
 * Inequality operator.
 */
template <typename T, typename Y>
inline bool
operator!=(const compound_pool_ptr<T> &lhs,
	   const compound_pool_ptr<Y> &rhs) noexcept
{
	return !(lhs == rhs);
}

/**
 * Inequality operator with nullptr.
 */
template <typename T>
inline bool
operator!=(const compound_pool_ptr<T> &lhs, std::nullptr_t) noexcept
{
	return lhs.raw() != 0;
}

/**
 * Inequality operator with nullptr.
 */
template <typename T>
inline bool
operator!=(std::nullptr_t, const compound_pool_ptr<T> &lhs) noexcept
{
	return lhs.raw() != 0;
}

/**
 * Equality operator with nullptr.
 */
template <typename T>
inline bool
operator==(const compound_pool_ptr<T> &lhs, std::nullptr_t) noexcept
{
	return lhs.raw() == 0;
}

/**
 * Equality operator with nullptr.
 */
template <typename T>
inline bool
operator==(std::nullptr_t, const compound_pool_ptr<T> &lhs) noexcept
{
	return lhs.raw() == 0;
}

} /* namespace detail */

} /* namespace pmem */

#endif