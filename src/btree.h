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

#ifndef __btree_h__
#define __btree_h__

#include "bnode.h"
#include "bnode_ptr.h"
#include "bnode_proxy.h"
#include "bcache.h"
#include "biter.h"
#include <boost/iterator/iterator_facade.hpp>

template<class Policy>
class btree
{
	typedef bnode<Policy> node_t;
	typedef typename apply_policy<Policy>::ptr_t ptr_t;
	typedef typename apply_policy<Policy>::cache_t cache_t;
	typedef typename Policy::key_t key_t;
	typedef typename Policy::value_t value_t;
public:
	btree() : m_cache(NULL) {}

	btree(cache_t& cache)
		: m_cache(&cache)
		, m_height(0)
	{}

	btree(cache_t& cache, const std::string& name)
		: m_cache(&cache)
		, m_height(0)
	{
		m_cache->get_root(name, m_root, m_height);
	}

	btree(const btree& rhs)
		: m_cache(rhs.m_cache)
		, m_root(rhs.m_root)
		, m_height(rhs.m_height)
	{}

	btree& operator=(const btree& rhs)
	{
		m_cache = rhs.m_cache;
		m_root = rhs.m_root;
		m_height = rhs.m_height;
		return *this;
	}

public:
	template<class Updater>
	bool update(const key_t& k, const Updater& updater)
	{
		assert(m_cache);
		// If root is null, see if an insert works
		if (m_root == ptr_t())
		{
			value_t v;
			bool exists = false;
			// Try the insert
			bool changed = updater(v, exists);
			// If nothing changed, return false
			if (!changed || !exists)
				return false;
			// Otherwise, create the initial node
			m_root = m_cache->new_node(new node_t(k, v));
			m_height++;
			return true;
		}

		// Let's try running the update
		node_t* w_root = m_root->copy();
		node_t* overflow = NULL;
		ptr_t peer;
		typename node_t::update_result r = w_root->update(*m_cache, k, peer, overflow, updater);

		if (r == node_t::ur_nop)
		{
			// If nothing happend, delete tmp root and return
			delete w_root;
			return false;
		}
		if (r == node_t::ur_modify)
		{
			// Easy case, just update main node
			m_root = m_cache->new_node(w_root);
		}
		else if (r == node_t::ur_split)
		{
			// Root just split, make new root
			m_root = m_cache->new_node(new node_t(
				m_height,
				m_cache->new_node(w_root), 
				m_cache->new_node(overflow)
			));
			m_height++;  
		} 
		else if (r == node_t::ur_singular)
		{
			// If the node is down to one element
			// Get it's inner bit and throw it away
			m_root = w_root->ptr(0);
			delete w_root;
			m_height--;
		}
		else if (r == node_t::ur_empty)
		{
			// Tree is totally empty
			m_root = ptr_t();
			delete w_root;
			m_height = 0;
		}
		clean_one();
		return true;	
	}

	void clear()
	{
		m_root = ptr_t();
		m_height = 0;
		m_cache = NULL;
	}

	typedef std::pair<key_t, value_t> value_type;
	class const_iterator : public boost::iterator_facade<
		const_iterator,
		const value_type,
		boost::bidirectional_traversal_tag>
	{
		friend class boost::iterator_core_access;
		friend class btree;
	public:
		const_iterator() {}
		const_iterator(const btree& self) 
			: m_state(self.m_root, self.m_height) 
		{}
	private:
		//btree& m_self;
		biter<Policy> m_state;
		void increment() { m_state.increment(); }
		void decrement() { m_state.decrement(); }
		bool equal(const_iterator const& other) const { return m_state == other.m_state; }
		const value_type& dereference() const { return m_state.get_pair(); }
		/*
		void maybe_update() const
		{
			if (m_self.m_root != m_state.get_root() || m_self.m_height != m_state.get_height())
			{
				key_t k = m_state.get_key();
				m_state = biter<Policy>(m_self.m_root, m_self.m_height);
				m_state.set_find(m_state.get_key());
			}
		}
		*/
	};

	friend class const_iterator;

	const_iterator begin() const { 
		const_iterator r(*this); r.m_state.set_begin(); return r; 
	}
	const_iterator end() const { 
		const_iterator r(*this); return r; 
	}
	const_iterator find(const key_t& k) const { 
		const_iterator r(*this); r.m_state.set_find(k); return r; 
	}
	const_iterator lower_bound(const key_t& k) const { 
		const_iterator r(*this); r.m_state.set_lower_bound(k); return r; 
	}
	const_iterator upper_bound(const key_t& k) const { 
		const_iterator r(*this); r.m_state.set_upper_bound(k); return r; 
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

	/*snapshot_t get_snapshot() const 
	{ 
		return snapshot_t(m_root, m_height); 
	}
	*/

	void sync(const std::string& name) { assert(m_cache); m_cache->sync(name, m_root, m_height); }

#ifdef __BTREE_DEBUG
	void print() const
	{
		if (m_root != ptr_t())
			m_root->print(0);
		printf("\n");
	}

	bool validate() const
	{
		if (m_root == ptr_t() && m_height == 0)
			return true;

		if (m_root == ptr_t() && m_height != 0)
		{
			printf("Root is null for non-zero height");
			return false;
		}
		return m_root->validate(m_height - 1, true);	
	}
#endif
private:
	off_t lowest_loc()
	{
		if (m_root == ptr_t())
			return std::numeric_limits<off_t>::max();
		return m_cache->get_oldest(m_root);
	}

	void clean_one()
	{
		m_cache->load_below(m_root, lowest_loc());
	}

	cache_t* m_cache;
	ptr_t m_root;
	size_t m_height;
};

#endif
