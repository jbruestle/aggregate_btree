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
#include <boost/foreach.hpp>
#define foreach BOOST_FOREACH

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
	typedef btree_base<Policy> tree_t;
	typedef boost::shared_ptr<tree_t> tree_ptr_t;
	typedef typename Policy::store_t store_t;
	typedef abt_lock lock_t;

	typedef std::map<std::string, tree_ptr_t> roots_t;
public:
	bcache(store_t& store, size_t max_unwritten_size, size_t max_lru_size, const Policy& policy = Policy())
		: m_store(store)
		, m_max_unwritten_size(max_unwritten_size)
		, m_max_lru_size(max_lru_size)
		, m_is_synced(false)
		, m_default_policy(policy)
	{
		std::vector<char> buf;
		m_store.read_root(buf);
		if (buf.size() == 0)
			return;
		vector_reader io(buf);
		size_t size;
		deserialize(io, size);
		for(size_t i = 0; i < size; i++)
		{
			std::string str;
			deserialize(io, str);
			off_t off;
			off_t oldest;
			size_t height;
			size_t size;
			deserialize(io, off);
			deserialize(io, oldest);
			deserialize(io, height);
			deserialize(io, size);
			ptr_t root;
			if (off != 0)
 				root = lookup(off, oldest, height, policy);
			m_current.insert(std::make_pair(str, 
				tree_ptr_t(new tree_t(this, root, height, size, policy))));
		}
	}

	~bcache()
	{
		while(m_lru.size() > 0)
			reduce_lru();
	}

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
			if (proxy.m_state == proxy_t::unwritten)  // If it's unwritten
			{
				// Remove from list to write, delete node and proxy
				m_unwritten.erase(m_unwritten.iterator_to(proxy));  
				delete proxy.m_ptr;
			}
			else if (proxy.m_state == proxy_t::unloaded)
			{
				// Just erase for offset 
				m_oldest.erase(&proxy);
				m_by_off.erase(proxy.m_off);
			}
			else if (proxy.m_state == proxy_t::cached)
			{
				m_oldest.erase(&proxy);
				m_by_off.erase(proxy.m_off);
				m_lru.erase(m_lru.iterator_to(proxy));
				delete proxy.m_ptr;
			}
			else
				assert(false);

			delete &proxy;
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
			// Create a new node to load into
			node_t* node = new node_t(proxy.m_policy, 0);
			// Do the actual load
			read_node(proxy.m_off, *node);
			proxy.m_ptr = node;
			proxy.m_state = proxy_t::cached; // Set state to cached
		}
		// Now node is in 'cached, or unwritten' state
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
		lock_t lock(m_mutex);
		assert(node != NULL);
		proxy_t* proxy = new proxy_t(*this, node); // Make the proxy
		m_unwritten.push_back(*proxy); // Add it to unwritten
		// Calculate m_oldest
		ptr_t r(proxy);
		// Write data if our buffer is full
		while(m_unwritten.size() > m_max_unwritten_size)
			write_front();
		// Return new pointer
		return r;
	}

	ptr_t lookup(off_t off, off_t oldest, size_t height, const Policy& policy)
	{
		lock_t lock(m_mutex);
		assert(off != 0);
		proxy_t* r;
		typename by_off_t::iterator it = m_by_off.find(off);
		if (it == m_by_off.end())
		{
			r = new proxy_t(*this, off, oldest, height, policy);
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

private:
	void update_policy(const ptr_t& ptr, const Policy& policy)
	{
		if (ptr.m_proxy == NULL) return;  // We are done
		proxy_t* proxy = ptr.m_proxy;
		proxy->m_policy = policy;
		if (proxy->m_ptr != NULL)
		{
			node_t* node = (node_t*) proxy->m_ptr;
			node->m_policy = policy;
			for(size_t i = 0; i < node->size(); i++)
			{
				update_policy(node->ptr(i), policy);
			}
		}
	}
public:

	tree_ptr_t attach(const std::string& name, const Policy& policy = Policy())
	{
		lock_t lock(m_mutex);
		if (m_current.find(name) == m_current.end())
		{
			// Make a new entry if needed
			m_current.insert(std::make_pair(name, tree_ptr_t(new tree_t(this, policy))));
		}
		else
		{
			// Find existing entry
			tree_ptr_t t = m_current[name]; 
			// Update the policy as needed
			t->set_policy(policy);
			update_policy(t->get_root(), policy);
		}
		// Return a the tree
		return m_current[name];
	}

	void copy_roots(roots_t& out, const roots_t& in)
	{
		out.clear();
		foreach(const typename roots_t::value_type& kvp, m_current)
			m_mark[kvp.first] = tree_ptr_t(new tree_t(*kvp.second));
	}

	void mark()
	{
		lock_t lock(m_mutex);
		copy_roots(m_mark, m_current);
		m_mark = m_current;
		m_is_synced = false;
	}

	void revert()
	{
		lock_t lock(m_mutex);
		copy_roots(m_current, m_mark);
	}
		
	void sync()
	{
		lock_t lock(m_mutex);
		// Maybe skip sync if no new marks
		if (m_is_synced) return;
		// Set up for post sync world
		copy_roots(m_syncing, m_mark);
		m_syncing = m_mark;
		m_is_synced = true;
		// Put in a 'sync node
		proxy_t* p = new proxy_t(*this, m_default_policy);
		m_unwritten.push_back(*p);
		// Go until I hit it
		while(p->m_state != proxy_t::root_marker_done)
			write_front();
		delete p;
		//  allow system to clear old data
		if (m_oldest.size())
		{
			off_t oldest = (*m_oldest.begin())->m_oldest;
			if (oldest != std::numeric_limits<off_t>::max())
				m_store.clear_before(oldest);
		}
		// Remove excess cached nodes
		while(m_lru.size() > m_max_lru_size)
			reduce_lru();
	}		

	void clean_one()
	{
		lock_t lock(m_mutex);
		off_t oldest = std::numeric_limits<off_t>::max();
		typename roots_t::const_iterator it, itEnd = m_current.end();
		// Find the oldest element that is current
		for(it = m_current.begin(); it != itEnd; ++it)
		{
			if (it->second->size() != 0)
				oldest = std::min(oldest, it->second->get_root().get_oldest());
		}
		// If it's not on disk, forget it
		if (oldest == std::numeric_limits<off_t>::max()) return;
		// Otherwise move it forward
		for(it = m_current.begin(); it != itEnd; ++it)
		{
			it->second->load_below(oldest);
		}
	}
	
private:
	void write_root(const roots_t& roots)
	{
               	std::vector<char> buf;
		vector_writer out(buf);
		serialize(out, roots.size());
		typename roots_t::const_iterator it, itEnd = roots.end();
		// Go over each entry
		for(it = roots.begin(); it != itEnd; ++it)
		{
			serialize(out, it->first);  // Write it's name
			const tree_ptr_t& t = it->second;  
			off_t off = 0;
			off_t oldest = 0;
			if (t->get_root() != ptr_t())
			{
				off = t->get_root().get_offset();
				oldest = t->get_root().get_oldest();
			}
			// Write the actual data
			serialize(out, off);
			serialize(out, oldest);
			serialize(out, t->get_height()); 
			serialize(out, t->size());
		}
		m_store.write_root(buf);
	}

	void write_front()
	{
		// Pop front writable node
		proxy_t& proxy = m_unwritten.front();
		m_unwritten.pop_front();
		// Handle special case of 'root' write
		if (proxy.m_state == proxy_t::root_marker)
		{
			write_root(m_syncing);
			proxy.m_state = proxy_t::root_marker_done;
		}
		else
		{
			// Do actual write
			off_t off = write_node(*proxy.m_ptr);
			// Update offset
			//m_oldest.erase(&proxy);
			proxy.m_off = off;
			proxy.m_oldest = off;
			const node_t* node = proxy.m_ptr;
			if (node->height() != 0)
			{
				for(size_t i = 0; i < node->size(); i++)
					proxy.m_oldest = std::min(proxy.m_oldest, node->ptr(i).get_oldest());
			}
			m_oldest.insert(&proxy);
			// Change state to cached
			proxy.m_state = proxy_t::cached;
			// Add to LRU if appropriate
			if (proxy.m_pin_count == 0)
				m_lru.push_back(proxy);
		}
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
			m_oldest.erase(&proxy);
			m_by_off.erase(proxy.m_off);
			delete &proxy;
		}
	}

        off_t write_node(const node_t& bnode)
        {
                std::vector<char> buf;
                vector_writer io(buf);
                bnode.serialize(io);
                off_t r = m_store.write_node(buf);
                return r;
        }

	void read_node(off_t loc, node_t& bnode) 
	{ 
		std::vector<char> buf; 
		m_store.read_node(loc, buf); 
		vector_reader io(buf); 
		bnode.deserialize(io, *this);
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

	store_t& m_store;
	size_t m_max_unwritten_size;
	size_t m_max_lru_size;
	boost::intrusive::list<proxy_t> m_unwritten;
	boost::intrusive::list<proxy_t> m_lru;
	typedef boost::unordered_map<off_t, proxy_t*> by_off_t;
	by_off_t m_by_off;
	typedef std::set<proxy_t*, cmp_oldest> oldest_t;
	oldest_t m_oldest;
	bool m_is_synced;
	roots_t m_current;
	roots_t m_mark;
	roots_t m_syncing;
	Policy m_default_policy;
	abt_mutex m_mutex;
};

template<class Policy>
class bcache_nop
{
public:
	bcache_nop() {}
	typedef bnode<Policy> node_t;
	typedef typename apply_policy<Policy>::ptr_t ptr_t;
	ptr_t new_node(node_t* node) { return ptr_t(node); }
	void clean_one() {}
};

}
#endif
