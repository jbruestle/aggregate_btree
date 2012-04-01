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

#ifndef __bnode_proxy_h__
#define __bnode_proxy_h__

#include <assert.h>
#include <boost/thread.hpp>
#include <boost/intrusive/list.hpp>

#include "bdecl.h"

// Pinned proxy is a helper class to
// pin and unpin a proxy to allow RAII
template<class Policy>
class pinned_proxy 
{
	typedef bnode<Policy> node_t; 
	typedef bnode_proxy<Policy> proxy_t; 
public:
	pinned_proxy(proxy_t* proxy)
		: m_proxy(proxy)
	{
		assert(m_proxy);
		m_proxy->m_cache.pin(*proxy);
	}
	~pinned_proxy()
	{
		m_proxy->m_cache.unpin(*m_proxy);
	}
	const node_t* operator->()
	{
		return m_proxy->m_ptr;
	}
private:
	proxy_t* m_proxy;
};

template<class Policy>
class bnode_proxy : public boost::intrusive::list_base_hook<>
{
	typedef bnode<Policy> node_t;
	typedef bnode_cache_ptr<Policy> ptr_t;
	typedef bcache<Policy> cache_t;
	typedef boost::condition_variable_any condition_t;
	friend class pinned_proxy<Policy>;
	friend class bcache<Policy>;

private:
	// Create a new proxy from a node
	bnode_proxy(cache_t& store, const node_t* rhs) 
		: m_cache(store)
		, m_state(unwritten)
		, m_ref_count(1)
		, m_pin_count(0)
		, m_ptr(rhs)
		, m_off(0)
		, m_oldest(std::numeric_limits<off_t>::max())
	{}

	// Create a new proxy from a disk location
	bnode_proxy(cache_t& store, off_t off, off_t oldest) 
		: m_cache(store)
		, m_state(unloaded)
		, m_ref_count(1)
		, m_pin_count(0)
		, m_ptr(NULL)
		, m_off(off)
		, m_oldest(oldest)
	{}

	~bnode_proxy()
	{}

public:
	void inc() 
	{ 
		m_cache.inc(*this);
	}
	void dec() 
	{
		m_cache.dec(*this);
	}
	
	off_t get_offset() const
	{
		return m_off;
	}

	off_t get_oldest() const
	{
		return m_oldest;
	}

private:
	enum proxy_state
	{
		unwritten,
		writing,
		cached,
		unloaded,
		loading
	};
	cache_t& m_cache;	
	proxy_state m_state;
	condition_t m_cond;
	int m_ref_count;
	int m_pin_count;
	const node_t* m_ptr;
	off_t m_off;
	off_t m_oldest;
};

#endif
