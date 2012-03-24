/*
    Aggregate btree implementation
    Copyright (C) 2012 Jeremy Bruestle

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, version 3.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __bsnapshot_h__
#define __bsnapshot_h__

#include <boost/iterator/iterator_facade.hpp>

#include "bdecl.h"

template<class Policy>
class bsnapshot
{
	typedef bnode_ptr<Policy> ptr_t;
	typedef typename Policy::key_t key_t;
	typedef typename Policy::value_t value_t;
	friend class btree<Policy>;
public:
	bsnapshot(const ptr_t& root, size_t height) 
		: m_root(root)
		, m_height(height) 
	{ }

	bsnapshot(const bsnapshot& rhs)
		: m_root(rhs.m_root)
		, m_height(rhs.m_height)
	{ }
	
	void operator=(const bsnapshot& rhs)
	{
		m_root = rhs.m_root;
		m_height = rhs.m_height;
	}

	void print()
	{
		m_root->print(0);
	}

	typedef std::pair<key_t, value_t> value_type;

	class const_iterator : public boost::iterator_facade<
		const_iterator,
		const value_type,
		boost::bidirectional_traversal_tag>
	{
		friend class boost::iterator_core_access;
		friend class bsnapshot;
	public:
		const_iterator() {}
		const_iterator(const ptr_t& root, size_t height) 
			: m_state(root, height) {}

	private:
		biter<Policy> m_state;
		void increment() { m_state.increment(); }
		void decrement() { m_state.decrement(); }
		bool equal(const_iterator const& other) const { return m_state == other.m_state; }
		const value_type& dereference() const { return m_state.get_pair(); }

	};

	const_iterator begin() const { 
		const_iterator r(m_root, m_height); r.m_state.set_begin(); return r; 
	}
	const_iterator end() const { 
		const_iterator r(m_root, m_height); return r; 
	}

	const_iterator find(const key_t& k) const { 
		const_iterator r(m_root, m_height); r.m_state.set_find(k); return r; 
	}
	const_iterator lower_bound(const key_t& k) const { 
		const_iterator r(m_root, m_height); r.m_state.set_lower_bound(k); return r; 
	}
	const_iterator upper_bound(const key_t& k) const { 
		const_iterator r(m_root, m_height); r.m_state.set_upper_bound(k); return r; 
	}

	// Logically, accumulate_until moves 'cur' forward, adding cur->second to total until either:
	// 1) cur == end
        // 2) adding cur->second to total would make threshold(total) true
	// That is, we stop right before threshold becomes true
	// In reality, the whole thing is done in log(n) time through fancy tricks
	template<class Functor>
	void accumulate_until(const_iterator& cur, value_t& total, const const_iterator& end, const Functor& threshold)
	{
		cur.m_state.accumulate_until(threshold, total, end.m_state);
	}
	
private:
	ptr_t m_root;
	size_t m_height;
};

#endif
