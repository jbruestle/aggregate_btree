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

#ifndef __bcache_h__
#define __bcache_h__

#include <set>
#include <boost/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/intrusive/list.hpp>

#include "bdecl.h"

template<class Key, class Value, class Context>
class bcache
{
	typedef bnode<Key, Value, Context> node_t;
	typedef bnode_proxy<Key, Value, Context> proxy_t;
	typedef bnode_ptr<Key, Value, Context> ptr_t;
	typedef bstore<Key, Value, Context> store_t;
	typedef boost::recursive_mutex mutex_t;
	typedef boost::unique_lock<mutex_t> lock_t;

public:
	bcache(size_t max_unwritten_size, size_t max_lru_size, const Context& context)
		: m_max_unwritten_size(max_unwritten_size)
		, m_max_lru_size(max_lru_size)
		, m_in_write(false)
		, m_context(context)
	{}

	void inc(proxy_t& proxy)
	{
		lock_t lock(m_mutex);
		proxy.m_ref_count++;
	}

	void dec(proxy_t& proxy)
	{
		lock_t lock(m_mutex);
		proxy.m_ref_count--;
		if (proxy.m_ref_count == 0)
		{
			assert(proxy.m_pin_count == 0);
			assert(proxy.m_state != proxy_t::writing && proxy.m_state != proxy_t::loading);
			if (proxy.m_state == proxy_t::unwritten)  // If it's unwritten
			{
				// Remove from list to write, delete node and proxy
				m_unwritten.erase(m_unwritten.iterator_to(proxy));  
				delete proxy.m_ptr; // May cause further 'dec' via recursion
				delete &proxy;
			}
			else // Must be unloaded or cached
			{
				// Remove from oldest list since it's no longer reachable
				old_off_t oot(proxy.m_oldest, &proxy);
				m_oldest.erase(oot);
				// If it's not in the cache, delete it completely
				if (proxy.m_state == proxy_t::unloaded)
				{
					m_by_off.erase(proxy.m_off);
					delete &proxy;
				}
			}
		}
	}


	void pin(proxy_t& proxy) 
	{
		lock_t lock(m_mutex);
		proxy.m_pin_count++;
		
		if (proxy.m_state == proxy_t::cached)
		{
			// First pinner removes cached nodes from LRU so they
			// aren't removed from under the pinning thread
 			if (proxy.m_pin_count == 1)
				m_lru.erase(m_lru.iterator_to(proxy));
		}
		else if (proxy.m_state == proxy_t::unloaded)
		{
			assert(proxy.m_pin_count == 1);
			proxy.m_state = proxy_t::loading; // Set state to loading
			m_mutex.unlock(); // Lock count should be exactly 0 (pin in nonrecursive)
			// Load node outside of lock
			node_t* node = new node_t(0);
			m_store.read_node(proxy.m_off, *node, *this);
			m_mutex.lock();  // Relock
			proxy.m_ptr = node;
			proxy.m_state = proxy_t::cached; // Set state to cached
			proxy.m_cond.notify_all(); // Let others know it's loaded
		}
		else if (proxy.m_state == proxy_t::loading)
		{
			// If it's already loading, wait until it's done
			while(proxy.m_state != proxy_t::cached)
				proxy.m_cond.wait(lock);  
		}
		// Now node is in 'cached, unwritten, or writing' state
		// All of which are totally good to read data in			
	}

	void unpin(proxy_t& proxy)
	{
		lock_t lock(m_mutex);
		proxy.m_pin_count--;
		if (proxy.m_pin_count == 0 && proxy.m_state == proxy_t::cached)
		{
			m_lru.push_back(proxy);  // Put at end of LRU
			while(m_lru.size() > m_max_lru_size)
				reduce_lru();  // Reduce if needed
		}
	}

	ptr_t new_node(node_t* node)
	{
		assert(node != NULL);
		lock_t lock(m_mutex);
		proxy_t* proxy = new proxy_t(*this, node); // Make the proxy
		m_unwritten.push_back(*proxy); // Add it to unwritten
		if (node->height() != 0)
		{
			for(size_t i = 0; i < node->size(); i++)
				proxy->m_oldest = std::min(proxy->m_oldest, node->ptr(i).get_oldest());
		}
		ptr_t r(proxy);
		while(m_unwritten.size() > m_max_unwritten_size)
			write_front(lock);
		return r;
	}

	ptr_t lookup(off_t off, off_t oldest)
	{
		assert(off != 0);
		lock_t lock(m_mutex);
		proxy_t* r;
		typename by_off_t::iterator it = m_by_off.find(off);
		if (it == m_by_off.end())
		{
			r = new proxy_t(*this, off, oldest);
			old_off_t oot(oldest, r);
			m_oldest.insert(oot);
			m_by_off[off] = r;
		}
		else
		{
			r = it->second;
			r->inc();
		}
		return ptr_t(r);
	}

	ptr_t open(const std::string& dir, bool create)
	{
		close();
		m_store.open(dir, create);
		off_t root_off = m_store.get_root();
		ptr_t r;
		if (root_off != 0)
		{
			off_t root_node;
			off_t root_oldest;
			m_store.read_root(root_off, root_node, root_oldest);
			r = lookup(root_node, root_oldest);
		}
		return r;
	}

	void sync(const ptr_t& until)
	{
		off_t ll;
		{
			lock_t lock(m_mutex);
			// If sync is empty, don't bother
			if (until == ptr_t()) 
				return;
			// Get the node to sync to
			proxy_t& proxy = *until.get_proxy();
			// While it's not written, write more nodes
			while(proxy.m_state == proxy_t::unwritten)
				write_front(lock);
			// Get it's info
			size_t off = proxy.m_off;
			size_t oldest = proxy.m_oldest;
			// Write the root info outside of the lock
			m_mutex.unlock();
			m_store.write_root(off, oldest);
			m_mutex.lock();
			// Remove excess cached nodes
			while(m_lru.size() > 0)
				reduce_lru();
			// Find the lowest location
			ll = lowest_loc();
		}
		// Clear excess written data outside of the lock
		m_store.clear_before(ll);
	}

	// Does writes
	void close()
	{
		lock_t lock(m_mutex);
		while(m_unwritten.size() > 0)
			write_front(lock);
		while(m_lru.size() > 0)
			reduce_lru();
		m_store.close();
	}

	const Context& get_context() { return m_context; }
private:
	off_t lowest_loc()
	{
		if (m_oldest.size() == 0)
		{
			return std::numeric_limits<off_t>::max();
		}
		return m_oldest.begin()->first;
	}

	void write_front(lock_t& lock)
	{
		while(m_in_write)
			m_cond.wait(lock);
		m_in_write = true;
		// Pop front writable node
		proxy_t& proxy = m_unwritten.front();
		m_unwritten.pop_front();
		// Prepare for write
		proxy.m_pin_count++;
		proxy.m_state = proxy_t::writing;
		const node_t* node = proxy.m_ptr;
		m_mutex.unlock();
		// Do actual write outside of lock
		off_t off = m_store.write_node(*node);
		m_mutex.lock();
		// Now change node state yet again
		m_in_write = false;
		m_cond.notify_all();
		proxy.m_off = off;
		proxy.m_oldest = off;
		if (node->height() != 0)
		{
			for(size_t i = 0; i < node->size(); i++)
				proxy.m_oldest = std::min(proxy.m_oldest, node->ptr(i).get_oldest());
		}
		old_off_t oot(proxy.get_oldest(), &proxy);
		m_oldest.insert(oot);
		proxy.m_state = proxy_t::cached;
		proxy.m_pin_count--;
		if (proxy.m_pin_count == 0)
			m_lru.push_back(proxy);
	}

	void reduce_lru()
	{
		proxy_t& proxy = m_lru.front();
		m_lru.pop_front();
		delete proxy.m_ptr;
		proxy.m_state = proxy_t::unloaded;
		proxy.m_ptr = NULL;
		if (proxy.m_ref_count == 0)
		{
			m_by_off.erase(proxy.m_off);
			delete &proxy;
		}
	}
		
	store_t m_store;
	mutex_t m_mutex;	
	size_t m_max_unwritten_size;
	size_t m_max_lru_size;
	bool m_in_write;
	boost::condition_variable_any m_cond;
	boost::intrusive::list<proxy_t> m_unwritten;
	boost::intrusive::list<proxy_t> m_lru;
	typedef boost::unordered_map<off_t, proxy_t*> by_off_t;
	by_off_t m_by_off;
	typedef std::pair<off_t, proxy_t*> old_off_t;
	std::set<old_off_t> m_oldest;
	Context m_context;
};

#endif