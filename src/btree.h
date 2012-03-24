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
#include "bstore.h"
#include "biter.h"
#include "bsnapshot.h"

template<class Policy>
class btree
{
	typedef bnode<Policy> node_t;
	typedef bnode_ptr<Policy> ptr_t;
	typedef typename Policy::key_t key_t;
	typedef typename Policy::value_t value_t;
	typedef typename Policy::context_t context_t;
public:
	typedef bsnapshot<Policy> snapshot_t;

	btree(size_t max_unwritten, size_t max_cache, const context_t& context = context_t()) 
		: m_cache(max_unwritten, max_cache, context)
		, m_height(0)
	{}

public:
	const context_t& get_context() { return m_cache.get_context(); }

	template<class Updater>
	bool update(const key_t& k, const Updater& updater)
	{
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
			m_root = m_cache.new_node(new node_t(k, v));
			m_height++;
			return true;
		}

		// Let's try running the update
		node_t* w_root = m_root.copy();
		node_t* overflow = NULL;
		ptr_t peer;
		typename node_t::update_result r = w_root->update(m_cache, k, peer, overflow, updater);

		if (r == node_t::ur_nop)
		{
			// If nothing happend, delete tmp root and return
			delete w_root;
			return false;
		}
		if (r == node_t::ur_modify)
		{
			// Easy case, just update main node
			m_root = m_cache.new_node(w_root);
		}
		else if (r == node_t::ur_split)
		{
			// Root just split, make new root
			m_root = m_cache.new_node(new node_t(
				m_height,
				m_cache.new_node(w_root), 
				m_cache.new_node(overflow)
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
		return true;	
	}

	off_t lowest_loc()
	{
		if (m_root == ptr_t())
			return std::numeric_limits<off_t>::max();
		return m_root.get_oldest();
	}

	bool load_below(off_t off) 
	{
		if (m_root == ptr_t())
			return false;

		// Otherwise, give it a go
		node_t* w_root = m_root.copy();
		bool r = w_root->load_below(m_cache, off);
		if (r)
			m_root = m_cache.new_node(w_root);
		else
			delete w_root;

		return r;
	}

	snapshot_t get_snapshot() const 
	{ 
		return snapshot_t(m_root, m_height); 
	}

	void print() const
	{
		if (m_root != ptr_t())
			m_root.print(0);
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
		return m_root.validate(m_height - 1, true);	
	}

	void open(const std::string& dir, bool create) 
	{
		close();
		m_root = m_cache.open(dir, create);
	}
	
	void close()
	{
		if (m_root != ptr_t())
			m_cache.sync(m_root);
		m_root = ptr_t();
		m_cache.close();
	}

	void sync()
	{
		m_cache.sync(m_root);
	}

	void sync(const snapshot_t& snapshot)
	{
		m_cache.sync(snapshot.m_root);
	}

private:
	mutable bcache<Policy> m_cache;
	ptr_t m_root;
	size_t m_height;
};

#endif
