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
#include <boost/unordered_map.hpp>
#include <boost/intrusive/list.hpp>

#include "abtree/bdecl.h"
#include "abtree/vector_io.h"
#include "abtree/abt_thread.h"

namespace btree_impl {

template<class Policy>
class bcache
{
	typedef bnode<Policy> node_t;
	typedef bnode_proxy<Policy> proxy_t;
	typedef bnode_cache_ptr<Policy> ptr_t;
	typedef abt_mutex mutex_t;
	typedef abt_lock lock_t;
	typedef typename Policy::store_t store_t;

public:
	bcache(store_t& store, size_t max_unwritten_size, size_t max_lru_size, const Policy& policy = Policy())
		: m_policy(policy)
		, m_store(store)
		, m_max_unwritten_size(max_unwritten_size)
		, m_max_lru_size(max_lru_size)
		, m_in_write(false)
	{}

	~bcache()
	{
		lock_t lock(m_mutex);
		while(m_lru.size() > 0)
			reduce_lru(lock);
	}

	void inc(proxy_t& proxy)
	{
		lock_t lock(m_mutex);
		proxy.m_ref_count++;
	}

	void dec(proxy_t& proxy)
	{
		std::vector<const node_t*> further;
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
					further.push_back(proxy.m_ptr);  // Prevent recursive 'dec' with lock held
					delete &proxy;
				}
				else // Must be unloaded or cached
				{
					// Remove from oldest list since it's no longer reachable
					m_oldest.erase(&proxy);
					// If it's not in the cache, delete it completely
					if (proxy.m_state == proxy_t::unloaded)
					{
						m_by_off.erase(proxy.m_off);
						delete &proxy;
					}
				}
			}
		}
		for(size_t i = 0; i < further.size(); i++)
			delete further[i];
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
			node_t* node = new node_t(m_policy, 0);
			read_node(proxy.m_off, *node);
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
				reduce_lru(lock);  // Reduce if needed
		}
	}

	Policy& get_policy() { return m_policy; }

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

	ptr_t lookup(off_t off, off_t oldest, size_t height)
	{
		assert(off != 0);
		lock_t lock(m_mutex);
		proxy_t* r;
		typename by_off_t::iterator it = m_by_off.find(off);
		if (it == m_by_off.end())
		{
			r = new proxy_t(*this, off, oldest, height);
			m_oldest.insert(r);
			m_by_off[off] = r;
		}
		else
		{
			r = it->second;
			r->m_ref_count++;
		}
		return ptr_t(r);
	}

	void get_root(const std::string& name, ptr_t& root, size_t& height, size_t& size)
	{
		off_t root_node;
		off_t root_oldest;
		read_root(name, root_node, root_oldest, height, size);
		if (root_node != 0)
			root = lookup(root_node, root_oldest, height);
	}
	
	void sync(const std::string& name, const ptr_t& until, size_t height, size_t size)
	{
		off_t ll;
		{
			lock_t lock(m_mutex);
			// If sync is empty, don't bother
			if (until == ptr_t()) 
			{
				std::vector<char> nothing;
				m_store.write_root(name, nothing);
				return;
			}
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
			write_root(name, off, oldest, height, size);
			m_mutex.lock();
			// Remove excess cached nodes
			while(m_lru.size() > m_max_lru_size)
				reduce_lru(lock);
			// Find the lowest location
			ll = std::numeric_limits<off_t>::max();
			if (m_oldest.size() != 0)
				ll = (*m_oldest.begin())->m_oldest;
		}
		// Clear excess written data outside of the lock
		m_store.clear_before(ll);
	}

	void clean_one()
	{
		lock_t lock(m_mutex);
		// If there is nothing on disk, forget it
		if (m_oldest.size() == 0) return;
		// Otherwise, find the oldest location
		typename oldest_t::const_iterator it = m_oldest.begin();
		off_t to_clean = (*it)->m_oldest;
		// If it's not on disk, forget it, note: I don't think this can happen, but let's be safe
		if (to_clean == std::numeric_limits<off_t>::max()) return;
		// For all the elements, load and collect
		// Because everything within the same oldest offset is sorted by descending height
		// newly loaded nodes will *always* appear after our current iterator, and the resulting
		// queue of found nodes will be in topological sort sort for the DAG
		std::vector<proxy_t*> found;
		while(it != m_oldest.end() && (*it)->m_oldest == to_clean)
		{
			m_mutex.unlock();
			pin(**it); // The dreaded **
			m_mutex.lock();
			found.push_back(*it);
			++it;	
		}
		// Since we unlocked a bunch, we need to double check that nothing untoward
		// happened while we were gathering data (specifically, newly added nodes, etc)
		bool failed = false;
		size_t i = 0;
		it = m_oldest.begin();
		while(!failed && it != m_oldest.end() && (*it)->m_oldest == to_clean)
		{
			if (i >= found.size() || found[i] != *it) 
				failed = true;
			i++;
			it++;
		}
		if (!failed)
		{
			// Do the actual instantanous update
			for(size_t i = found.size(); i > 0; i--)
			{
				// Get each proxy, starting from the leaf
				proxy_t* p = found[i-1];
				// All node's state should be 'cached' and 'pinned'
				// They are pinned because we pinned them, they are cached because
				// only cached nodes ever appear in m_oldest
				m_oldest.erase(p); // Remove from oldest
				m_by_off.erase(p->m_off);  // Remove for offset lookup
				p->m_state = proxy_t::unwritten;  // Set state to unwritten
				m_unwritten.push_back(*p);  // Write to unwritten queue
			}
		}			
		// Regardless, unpin all the nodes
		m_mutex.unlock();
		for(size_t i = 0; i < found.size(); i++)
		{
			unpin(*found[i]);
		}
		m_mutex.lock();
		// Finally, remove extra unwritten data
		while(m_unwritten.size() > m_max_unwritten_size)
			write_front(lock);
	}
private:

	void write_front(lock_t& lock)
	{
		while(m_in_write)
			m_write_cond.wait(lock);
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
		off_t off = write_node(*node);
		m_mutex.lock();
		// Now change node state yet again
		m_in_write = false;
		m_write_cond.notify_all();
		proxy.m_off = off;
		proxy.m_oldest = off;
		if (node->height() != 0)
		{
			for(size_t i = 0; i < node->size(); i++)
				proxy.m_oldest = std::min(proxy.m_oldest, node->ptr(i).get_oldest());
		}
		m_oldest.insert(&proxy);
		proxy.m_state = proxy_t::cached;
		proxy.m_pin_count--;
		if (proxy.m_pin_count == 0)
			m_lru.push_back(proxy);
	}

	void reduce_lru(lock_t& lock)
	{
		proxy_t& proxy = m_lru.front();
		m_lru.pop_front();
		const node_t* to_delete = proxy.m_ptr;
		proxy.m_state = proxy_t::unloaded;
		proxy.m_ptr = NULL;
		if (proxy.m_ref_count == 0)
		{
			m_by_off.erase(proxy.m_off);
			delete &proxy;
		}
		m_mutex.unlock();
		delete to_delete;
		m_mutex.lock();
	}

        off_t write_node(const node_t& bnode)
        {
                std::vector<char> buf;
                vector_writer io(buf);
                bnode.serialize(io);
                off_t r = m_store.write_node(buf);
                return r;
        }

        void write_root(const std::string& name, off_t off, off_t oldest, size_t height, size_t size)
        {
                std::vector<char> buf;
                vector_writer io(buf);
                serialize(io, off);
                serialize(io, oldest); 
                serialize(io, height); 
                serialize(io, size); 
                m_store.write_root(name, buf);
        }

	void read_node(off_t loc, node_t& bnode) 
	{ 
		std::vector<char> buf; 
		m_store.read_node(loc, buf); 
		vector_reader io(buf); 
		bnode.deserialize(io, *this);
	}

	void read_root(const std::string& name, off_t& off, off_t& oldest, size_t& height, size_t& size)
	{
		std::vector<char> buf;
		m_store.read_root(name, buf);
		if (buf.size() == 0)
		{
			off = 0;
			oldest = std::numeric_limits<off_t>::max();
			height = 0;
			return;
		}
		vector_reader io(buf);
		deserialize(io, off);
		deserialize(io, oldest);
		deserialize(io, height);
		deserialize(io, size);
	}

	struct cmp_oldest
	{
		bool operator()(const proxy_t* a, const proxy_t* b) const
		{
			if (a->m_oldest != b->m_oldest) 
				return a->m_oldest < b->m_oldest;	
			if (a->m_height != b->m_height)
				return a->m_height > b->m_height;
			return a < b;
		}
	};

	Policy m_policy;		
	store_t& m_store;
	mutex_t m_mutex;	
	size_t m_max_unwritten_size;
	size_t m_max_lru_size;
	bool m_in_write;
	abt_condition m_write_cond;
	boost::intrusive::list<proxy_t> m_unwritten;
	boost::intrusive::list<proxy_t> m_lru;
	typedef boost::unordered_map<off_t, proxy_t*> by_off_t;
	by_off_t m_by_off;
	typedef std::set<proxy_t*, cmp_oldest> oldest_t;
	oldest_t m_oldest;
};

template<class Policy>
class bcache_nop
{
public:
	bcache_nop(const Policy& policy = Policy()) : m_policy(policy) {}
	typedef bnode<Policy> node_t;
	typedef typename apply_policy<Policy>::ptr_t ptr_t;
	ptr_t new_node(node_t* node) { return ptr_t(node); }
	void clean_one() {}
	Policy& get_policy() { return m_policy; }
private:
	Policy m_policy;
};

}
#endif
