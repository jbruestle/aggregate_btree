/*
    Aggregate btree_base implementation
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

#ifndef __btree_base_h__
#define __btree_base_h__

#include "abtree/bnode.h"
#include "abtree/bnode_ptr.h"
#include "abtree/bnode_proxy.h"
#include "abtree/bcache.h"
#include "abtree/biter.h"
#include "abtree/file_bstore.h"
#include <boost/iterator/iterator_facade.hpp>

namespace btree_impl {

template<class Policy>
class btree_base
{
	typedef bnode<Policy> node_t;
	typedef typename apply_policy<Policy>::ptr_t ptr_t;
	typedef typename apply_policy<Policy>::cache_t cache_t;
	typedef typename apply_policy<Policy>::cache_ptr_t cache_ptr_t;
	typedef typename Policy::value_t data_t;

public:
	typedef typename Policy::key_t key_type;
	typedef typename Policy::value_t mapped_type;
	typedef std::pair<key_type, data_t> value_type;
	typedef size_t size_type;
	
	btree_base(cache_ptr_t cache)
		: m_cache(cache)
		, m_height(0)
		, m_size(0)
	{}

	btree_base(cache_ptr_t cache, const std::string& name)
		: m_cache(cache)
		, m_height(0)
		, m_size(0)
	{
		m_cache->get_root(name, m_root, m_height, m_size);
	}

	btree_base(const btree_base& rhs)
		: m_cache(rhs.m_cache)
		, m_root(rhs.m_root)
		, m_height(rhs.m_height)
		, m_size(rhs.m_size)
	{}

	btree_base& operator=(const btree_base& rhs)
	{
		m_cache = rhs.m_cache;
		m_root = rhs.m_root;
		m_height = rhs.m_height;
		m_size = rhs.m_size;
		return *this;
	}

	template<class Updater>
	bool update(const key_type& k, const Updater& updater)
	{
		assert(m_cache);
		// If root is null, see if an insert works
		if (m_root == ptr_t())
		{
			data_t v;
			bool exists = false;
			// Try the insert
			bool changed = updater(v, exists);
			// If nothing changed, return false
			if (!changed || !exists)
				return false;
			// Otherwise, create the initial node
			m_root = m_cache->new_node(new node_t(m_cache->get_policy(), k, v));
			m_height++;
			m_size++;
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
		else if (r == node_t::ur_modify)
		{
			m_root = m_cache->new_node(w_root);
		}
		else if (r == node_t::ur_insert)
		{
			m_size++;
			m_root = m_cache->new_node(w_root);
		}
		else if (r == node_t::ur_erase)
		{
			m_size--;
			m_root = m_cache->new_node(w_root);
		}
		else if (r == node_t::ur_split)
		{
			// Root just split, make new root
			m_root = m_cache->new_node(new node_t(
				m_cache->get_policy(),
				m_height,
				m_cache->new_node(w_root), 
				m_cache->new_node(overflow)
			));
			m_height++;  
			m_size++;
		} 
		else if (r == node_t::ur_singular)
		{
			// If the node is down to one element
			// Get it's inner bit and throw it away
			m_root = w_root->ptr(0);
			delete w_root;
			m_height--;
			m_size--;
		}
		else if (r == node_t::ur_empty)
		{
			// Tree is totally empty
			m_root = ptr_t();
			delete w_root;
			m_height = 0;
			m_size = 0;
		}
		clean_one();
		return true;	
	}
	
private:
	class mutable_iterator;

public:
	class const_iterator : public boost::iterator_facade<
		const_iterator,
		const value_type,
		boost::bidirectional_traversal_tag>
	{
		friend class boost::iterator_core_access;
		friend class btree_base;
		friend class mutable_iterator;

		const_iterator(const btree_base& self) 
			: m_self(&self)
			, m_state(self.m_root, self.m_height) 
		{}
	public:
		const_iterator() : m_self(NULL) {}
		const_iterator(const const_iterator& rhs)
			: m_self(rhs.m_self)
			, m_state(rhs.m_state)
		{}
		const_iterator(const mutable_iterator& rhs)
			: m_self(rhs.m_iterator.m_self)
			, m_state(rhs.m_iterator.m_state)
		{}

		const_iterator& operator=(const const_iterator& rhs)
		{
			m_self = rhs.m_self;
			m_state = rhs.m_state;
			return *this;
		}
		const_iterator& operator=(const mutable_iterator& rhs)
		{
			m_self = rhs.m_iterator.m_self;
			m_state = rhs.m_iterator.m_state;
			return *this;
		}
	private:
		const btree_base* m_self;
		mutable biter<Policy> m_state;
		void increment() { maybe_update(); m_state.increment(); }
		void decrement() { maybe_update(); m_state.decrement(); }
		bool equal(const_iterator const& other) const { 
			if (m_self != other.m_self) return false;
			if (m_state.is_end() || other.m_state.is_end())
				return (m_state.is_end() && other.m_state.is_end());
			return m_state.get_key() == other.m_state.get_key();
		}
		const value_type& dereference() const { maybe_update(); return m_state.get_pair(); }
		void maybe_update() const { 
			if (m_self == NULL) return;
			if (m_self->m_root == m_state.get_root()) return;
			biter<Policy> next(m_self->m_root, m_self->m_height);
			next.set_find(m_state.get_key());
			m_state = next;
		}
	};

private:
	class data_proxy
	{
		friend class pair_proxy;
		friend class btree_base;
		data_proxy(btree_base& tree, const key_type& key, const mapped_type& data)
			: m_tree(tree)
			, m_key(key)
			, m_data(data)
		{}
	public:
		operator data_t() { return m_data; }
		data_proxy& operator=(const data_t& rhs) { m_tree.set(m_key, rhs); return *this; }
	private:
		btree_base& m_tree;
		key_type m_key;
		mapped_type m_data;
	};

	class pair_proxy
	{
		friend class mutable_iterator;
		pair_proxy(btree_base& tree, const key_type& key, const mapped_type& data)
			: second(tree, key, data)
			, first(second.m_key)
		{}
	public:
		pair_proxy* operator->() { return this; }
		data_proxy second;
		const key_type& first;
	};
private:
	class mutable_iterator : public boost::iterator_facade<
		mutable_iterator,
		value_type,
		boost::bidirectional_traversal_tag>
	{
		friend class boost::iterator_core_access;
		friend class btree_base;
		friend class const_iterator;
		mutable_iterator(btree_base& self) : m_iterator(self) 
		{}
	public:
		mutable_iterator() {}
		mutable_iterator(const mutable_iterator& rhs) : m_iterator(rhs.m_iterator) {}
		mutable_iterator& operator=(const mutable_iterator& rhs) 
		{
			m_iterator = rhs.m_iterator;
			return *this;
		}
		pair_proxy operator->() const
		{
			return pair_proxy(
				const_cast<btree_base&>(*m_iterator.m_self), 
				m_iterator.m_state.get_key(),
				m_iterator.m_state.get_value()
			);
		}
	private:
		const_iterator m_iterator;
		void increment() { m_iterator++; }
		void decrement() { m_iterator--; }
		bool equal(mutable_iterator const& other) const { return m_iterator == other.m_iterator; }
	};

	friend class mutable_iterator;
	friend class const_iterator;
public:
	typedef mutable_iterator iterator;

	class always_update
	{
	public:
		always_update(const mapped_type& data) : m_data(data) {}
		bool operator()(mapped_type& out, bool& exists) const 
		{
			out = m_data; 
			exists = true; 
			return true; 
		}
	private:
		mapped_type m_data;
	};

	class only_insert
	{
	public:
		only_insert(const mapped_type& data) : m_data(data) {}
		bool operator()(mapped_type& out, bool& exists) const 
		{
			if (exists) return false;
			out = m_data; 
			exists = true; 
			return true; 
		}
	private:
		mapped_type m_data;
	};

	class always_erase
	{
	public:
		always_erase() {}
		bool operator()(mapped_type& out, bool& exists) const 
		{
			exists = false; 
			return true; 
		}
	};

	void set(const key_type& key, const mapped_type& data)
	{
		update(key, always_update(data));
	}

	void clear()
	{
		m_root = ptr_t();
		m_height = 0;
	}

	bool empty()
	{
		return m_root == ptr_t();
	}

	size_t size() const
	{
		return m_size;
	}

	std::pair<iterator, bool> insert(const value_type& pair)
	{
		bool inserted = update(pair.first, only_insert(pair.value));
		iterator it = find(pair.first);
		return std::make_pair(it, inserted);
	}

	// Note: This is no faster than a regular insert
	iterator insert(const iterator& position, const value_type& pair)
	{
		return insert(pair).first;
	}

	template <class InputIterator>
	void insert(InputIterator first, InputIterator last)
	{
		while(first != last)
		{
			insert(*first);
			++first;
		}
	}

	void erase(iterator it)
	{
		update(it->first, always_erase());
	}

	size_t erase(const key_type& key)
	{
		if (update(key, always_erase()))
			return 1;
		else
			return 0;
	}

	// TODO: This could be done faster (log(N) always regardless of length of range)
	void erase(iterator it, iterator end)
	{
		while(it != end)
		{
			iterator next = it;
			++next;
			erase(it);
			it = next;
		}
	}

	data_proxy operator[](const key_type& k)
	{
		const_iterator r = find(k); 
		return data_proxy(*this, k, r.m_state.get_value());	
	}

	void swap(btree_base& other)
	{
		std::swap(m_cache, other.m_cache);		
		std::swap(m_root, other.m_root);		
		std::swap(m_height, other.m_height);		
		std::swap(m_size, other.m_size);		
	}
		
	const_iterator begin() const { 
		const_iterator r(*this); r.m_state.set_begin(); return r; 
	}
	iterator begin() { 
		iterator r(*this); r.m_iterator.m_state.set_begin(); return r; 
	}
	const_iterator end() const { 
		const_iterator r(*this); return r; 
	}
	iterator end() { 
		iterator r(*this); return r; 
	}
	const_iterator find(const key_type& k) const { 
		const_iterator r(*this); r.m_state.set_find(k); return r; 
	}
	iterator find(const key_type& k) { 
		iterator r(*this); r.m_iterator.m_state.set_find(k); return r; 
	}
	const_iterator lower_bound(const key_type& k) const { 
		const_iterator r(*this); r.m_state.set_lower_bound(k); return r; 
	}
	iterator lower_bound(const key_type& k) { 
		iterator r(*this); r.m_iterator.m_state.set_lower_bound(k); return r; 
	}
	const_iterator upper_bound(const key_type& k) const { 
		const_iterator r(*this); r.m_state.set_upper_bound(k); return r; 
	}
	iterator upper_bound(const key_type& k) { 
		iterator r(*this); r.m_iterator.m_state.set_upper_bound(k); return r; 
	}
	std::pair<const_iterator,const_iterator> equal_range(const key_type& x) const {
		return std::make_pair(lower_bound(x), upper_bound(x));
	}
	std::pair<iterator,iterator> equal_range(const key_type& x) {
		return std::make_pair(lower_bound(x), upper_bound(x));
	}
	
	size_t count(const key_type& x)
	{
		const_iterator it = find(x);
		if (it == end()) return 0;
		else return 1;
	}
	

	// Logically, accumulate_until moves 'cur' forward, adding cur->second to total until either:
	// 1) cur == end
	// 2) adding cur->second to total would make threshold(total) true
	// That is, we stop right before threshold becomes true
	// In reality, the whole thing is done in log(n) time through fancy tricks
	template<class Functor>
	void accumulate_until(const_iterator& cur, data_t& total, const const_iterator& end, const Functor& threshold)
	{
		cur.m_state.accumulate_until(threshold, total, end.m_state);
	}

	template<class Functor>
	void accumulate_until(iterator& cur, data_t& total, const const_iterator& end, const Functor& threshold)
	{
		cur.m_iterator.m_state.accumulate_until(threshold, total, end.m_state);
	}

	class forever_functor
	{
	public:
		forever_functor() {}
		bool operator()(const int& total) const { return false; }
	};

	data_t total(const const_iterator& start, const const_iterator& end)
	{
		data_t r = data_t();
		const_iterator it = start;
		accumulate_until(it, r, end, forever_functor());
		return r;
	}

	void sync(const std::string& name) { assert(m_cache); m_cache->sync(name, m_root, m_height, m_size); }

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

	cache_ptr_t m_cache;
	ptr_t m_root;
	size_t m_height;
	size_t m_size;
};

template<class Value>
class plus_equal
{
public:
	void operator()(Value& out, const Value& in) { out += in; }
};

template<class Key, class Value, class Aggregate, class Compare>
class memory_policy
{
public:
	static const bool use_cache = false;
	static const size_t min_size = 2;
	static const size_t max_size = 4;
	typedef Key key_t;
	typedef Value value_t;
	memory_policy(const Aggregate& _aggregate, const Compare& _less)
		: aggregate(_aggregate), less(_less)
	{}
	Aggregate aggregate;
	Compare less;
};

template<class BasePolicy, class File>
class disk_policy
{
public:
	static const bool use_cache = true;
	static const size_t min_size = BasePolicy::node_size/2;
	static const size_t max_size = (BasePolicy::node_size/2)*2;
	typedef typename BasePolicy::key_type key_t;
	typedef typename BasePolicy::mapped_type value_t;
	typedef File store_t;
	disk_policy(const BasePolicy& policy) : m_policy(policy) {}
	bool less(const key_t& a, const key_t& b) const { return m_policy.less(a,b); }
	void aggregate(value_t& out, const value_t& in) const { return m_policy.aggregate(out, in); }
	void serialize(writable& out, const key_t& k, const value_t& v) const { return m_policy.serialize(out, k, v); }
	void deserialize(readable& in, key_t& k, value_t& v) const { return m_policy.deserialize(in, k, v); }
private:
	BasePolicy m_policy;
};

}

#endif
