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

namespace btree_impl {

template<class Policy>
class bnode_cache_ptr
{
	typedef bnode<Policy> node_t;
	typedef bnode_proxy<Policy> proxy_t;
	typedef pinned_proxy<Policy> pinned_t;
public:
	bnode_cache_ptr() : m_proxy(NULL) {}
	bnode_cache_ptr(proxy_t* proxy) : m_proxy(proxy) {}
	bnode_cache_ptr(const bnode_cache_ptr& rhs) : m_proxy(rhs.m_proxy) { inc(); }
	bnode_cache_ptr& operator=(const bnode_cache_ptr& rhs) 
	{ 
		if (m_proxy != rhs.m_proxy)
		{
			dec();
			m_proxy = rhs.m_proxy;
			inc();
		}
		return *this;
	}
	~bnode_cache_ptr() { dec(); }
	bool operator==(const bnode_cache_ptr& rhs) const { return m_proxy == rhs.m_proxy; }
	bool operator!=(const bnode_cache_ptr& rhs) const { return m_proxy != rhs.m_proxy; }
	void clear() { dec(); m_proxy = NULL; }

	// HACK, used only for sync, fix this
	proxy_t* get_proxy() const { return m_proxy; }
	pinned_t operator->() const { return pinned_t(m_proxy); }

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

}
#endif
