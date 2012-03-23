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

#ifndef __bnode_ptr_h__
#define __bnode_ptr_h__

#include <assert.h>
#include <pthread.h>

#include "bdecl.h"

template<class Key, class Value, class Context>
class bnode_ptr
{
	typedef bnode<Key, Value, Context> node_t;
	typedef bnode_proxy<Key, Value, Context> proxy_t;
	typedef pinned_proxy<Key, Value, Context> pinned_t;
public:
	bnode_ptr() : m_proxy(NULL) {}
	bnode_ptr(proxy_t* proxy) : m_proxy(proxy) {}
	bnode_ptr(const bnode_ptr& rhs) : m_proxy(rhs.m_proxy) { inc(); }
	bnode_ptr& operator=(const bnode_ptr& rhs) 
	{ 
		if (m_proxy != rhs.m_proxy)
		{
			dec();
			m_proxy = rhs.m_proxy;
			inc();
		}
		return *this;
	}
	~bnode_ptr() { dec(); }
	bool operator==(const bnode_ptr& rhs) const { return m_proxy == rhs.m_proxy; }
	bool operator!=(const bnode_ptr& rhs) const { return m_proxy != rhs.m_proxy; }
	void clear() { dec(); m_proxy = NULL; }

	// HACK, used only for sync, fix this
	proxy_t* get_proxy() const { return m_proxy; }

	size_t size() const { 
		pinned_t pp(m_proxy);
		return pp->size();
	}
	Key key(size_t i) const { 
		pinned_t pp(m_proxy);
		return pp->key(i); 
	}
	Value val(size_t i) const { 
		pinned_t pp(m_proxy);
		return pp->val(i); 
	}
	Value total() const { 
		pinned_t pp(m_proxy);
		return pp->total(); 
	}
	bnode_ptr ptr(size_t i) const 
	{ 
		pinned_t pp(m_proxy);
		return pp->ptr(i); 
	}
	size_t lower_bound(const Key& k) const { 
		pinned_t pp(m_proxy);
		return pp->lower_bound(k); 
	}
	size_t upper_bound(const Key& k) const { 
		pinned_t pp(m_proxy);
		return pp->upper_bound(k); 
	}
	node_t *copy() const { 
		pinned_t pp(m_proxy);
		return pp->copy(); 
	}

	void print(int indent) const { 
		pinned_t pp(m_proxy);
		return pp->print(indent); 
	}

	bool validate(int goal_height, bool is_root) const { 
		pinned_t pp(m_proxy);
		return pp->validate(goal_height, is_root); 
	}

	Value compute_total() const { 
		pinned_t pp(m_proxy);
		return pp->compute_total(); 
	}

	off_t get_offset() const {
		assert(m_proxy);
		return m_proxy->get_offset();
	}
	off_t get_oldest() const {
		assert(m_proxy);
		return m_proxy->get_oldest();
	}
	
private:
	void inc() { if (m_proxy) m_proxy->inc(); }
	void dec() { if (m_proxy) m_proxy->dec(); }

	proxy_t* m_proxy;
};

#endif
