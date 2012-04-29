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

#ifndef __bnode_h__
#define __bnode_h__

#include <map>
#include <stdio.h>
#include <boost/optional.hpp>
#include <boost/checked_delete.hpp>

#include "abtree/bdecl.h"
#include "abtree/serial.h"

namespace btree_impl {

#ifdef __BTREE_DEBUG
extern size_t g_node_count;
#endif

template<class Policy>
class bnode 
{
private:
	// Largest and smallest legal node sizes
	const static size_t min_size = Policy::min_size;
	const static size_t max_size = Policy::max_size;
	typedef typename Policy::key_t key_t;
	typedef typename Policy::value_t value_t;
	typedef typename apply_policy<Policy>::ptr_t ptr_t;
	typedef typename apply_policy<Policy>::cache_t cache_t;
	typedef boost::optional<key_t> okey_t;
	typedef boost::optional<value_t> ovalue_t;
	friend class btree_base<Policy>;
	friend class apply_policy<Policy>::cache_t;
	friend class apply_policy<Policy>::ptr_t;
	friend void boost::checked_delete<>(bnode*);

	class functor_helper  // Allows policy 'less' to be a normal member function (or a functor)
	{
	public:
		functor_helper(Policy& policy) : m_policy(policy) {}
		bool operator()(const okey_t& k1, const key_t& k2) { return m_policy.less(*k1, k2); }
		bool operator()(const key_t& k1, const okey_t& k2) { return m_policy.less(k1, *k2); }
	private:
		Policy& m_policy;
	};

	// Create a new 'tree' with one element
	bnode(Policy& policy, const key_t& k, const value_t& v)
		: m_policy(policy)
		, m_height(0)      // We are a leaf
		, m_total(v)       // Total is our one entry
		, m_size(1)
	{
#ifdef __BTREE_DEBUG
		g_node_count++;
#endif
		m_keys[0] = k;
		m_values[0] = v;
	}

	// Make a new root node based on two nodes
	bnode(Policy& policy, size_t height, const ptr_t& n1, const ptr_t& n2)
		: m_policy(policy)
		, m_height(height)  // Compute our height
		, m_total(n1->total())  // Compute an initial total
		, m_size(2)
	{
		m_policy.aggregate(m_total, n2->total());
#ifdef __BTREE_DEBUG
		g_node_count++;
#endif
		assign(0, n1);
		assign(1, n2);
	}
	
	~bnode() {
#ifdef __BTREE_DEBUG
		g_node_count--; 
#endif
	}

	void serialize(writable& out) const
	{
		::serialize(out, m_height);
		::serialize(out, size());
		for(size_t i = 0; i < size(); i++)
		{
			m_policy.serialize(out, key(i), val(i));
			if (m_height != 0)
			{
				off_t child_loc = ptr(i).get_offset();
				off_t child_oldest = ptr(i).get_offset();
				::serialize(out, child_loc);
				::serialize(out, child_oldest);
			}
		}
	}

	void deserialize(readable& in, cache_t& cache)
	{
		::deserialize(in, m_height);
		size_t count;
		::deserialize(in, count);
		for(size_t i = 0; i < count; i++)
		{
			key_t key;
			value_t val;
			m_policy.deserialize(in, key, val);
			if (m_height != 0)
			{
				off_t child_loc;
				off_t child_oldest;
				::deserialize(in, child_loc);
				::deserialize(in, child_oldest);
				insert(key, val, cache.lookup(child_loc, child_oldest, m_height - 1));
			}
			else
				insert(key, val, ptr_t());
		}
	}

	// Return a writable version of this node
	bnode* copy() const
	{
		// Make a copy of a node	
		bnode* copy = new bnode(m_policy, m_height);
		copy->m_total = m_total;
		copy->m_size = m_size;
		for(size_t i = 0; i < size(); i++)
		{
			copy->m_keys[i] = m_keys[i];
			copy->m_values[i] = m_values[i];
			copy->m_ptrs[i] = m_ptrs[i];
		}

		// Return the new node
		return copy;
	}

	// What happened during the erase
	enum update_result 
	{
		ur_nop,  // Didn't make any changes
		ur_modify, // Just altered the entry
		ur_insert, // Inserted an entry, but no need for split
		ur_erase, // Erased an entry, but no effect on peer
		ur_split,  // Had to split the node
		ur_steal, // Erased and stole and entry from peer
		ur_merge, // Erased and merged with peer
		ur_singular, // Erased, had no peer, and down to a single down pointer
		ur_empty, // Node is both empty and has height of 0
	};

	template<class Updater> 
	update_result update(cache_t& cache, const key_t& k, ptr_t& peer, bnode*& split, const Updater& updater)
	{
		if (m_height == 0)
		{
			// This node is a leaf
			// Declare variables for updater
			bool exists = true;
			value_t v;

			// Try to find existing entry
			size_t i = find(k);
			if (i == size())
				exists = false;
			else
				v = val(i);

			// Run the updater
			bool did_exist = exists;
			bool changed = updater(v, exists);

			// Return if no actual change
			if (changed == false || (!did_exist && !exists))
				return ur_nop;

			if (did_exist && !exists)
			{
				// Erase case, remove element
				erase(i);
				// Run fixup
				return erase_fixup(cache, peer);
			}
			else if (!did_exist && exists)
			{
				// Insert case, do insert
				insert(k, v, ptr_t()); 
				// Maybe do a split
				split = maybe_split(); 
				return split ? ur_split : ur_insert;
			}
			else 
			{
				// Modify case
				m_values[i] = v; 
				recompute_total();
				return ur_modify;
			}
		}
		// Find the child node the update goes to
		size_t i = find_by_key(k);  
		// Find a peer, try next node over
		size_t pi = (i == size()-1 ? i - 1 : i+1);

		// Prepare node and it's peer for modification
		bnode* new_node = ptr(i)->copy();
		bnode* overflow = NULL;

		// Run the recursive update 
		update_result r = new_node->update(cache, k, ptrnc(pi), overflow, updater);
		if (r == ur_nop)
		{
			// Nothing happened, undo everything
			delete new_node;
			return ur_nop;
		}
		if (r == ur_modify || r == ur_erase || r == ur_insert)
		{
			// Easy case, keep new node, peer is untouched
			assign(i,cache.new_node(new_node));
			recompute_total();  // Recompute self
			return r;  // Send status up
		}
		if (r == ur_split)
		{
			// We had a split of the lower node
			// First keep new_node
			assign(i,cache.new_node(new_node));
			// Then add overflow node
			insert(cache.new_node(overflow));
			// Check for yet another split
			split = maybe_split();
			return split ? ur_split : ur_insert;
		}
		// We modified peer, update info
		m_keys[pi] = m_ptrs[pi]->key(0);
		m_values[pi] = m_ptrs[pi]->total();
		if (r == ur_steal)
		{
			// Keep new node
			assign(i, cache.new_node(new_node));
			recompute_total();  // Recompute self
			return ur_erase;  // Send up status
		}
		// r == ur_merge
		// Keep only peer (main down ptr is deleted)
		delete new_node;
		erase(i);  // Delete erased down ptr
		return erase_fixup(cache, peer);  // Do an erase fixup
	}

	ptr_t& ptrnc(size_t i) { return m_ptrs[i]; } 

public:
	Policy& get_policy() const { return m_policy; }
	size_t size() const { return m_size; }
	size_t height() const { return m_height; }
	const key_t& key(size_t i) const { return *m_keys[i]; }
	const value_t& val(size_t i) const { return *m_values[i]; }
	const value_t& total() const { return m_total; }
	const ptr_t& ptr(size_t i) const { return m_ptrs[i]; } 

	size_t lower_bound(const key_t& k) const 
	{ return std::lower_bound(m_keys, m_keys + m_size, k, functor_helper(m_policy)) - m_keys; }
	size_t upper_bound(const key_t& k) const 
	{ return std::upper_bound(m_keys, m_keys + m_size, k, functor_helper(m_policy)) - m_keys; }
	size_t find(const key_t& k) const
	{
		size_t i = lower_bound(k);
		if (i != m_size && !m_policy.less(k, *m_keys[i])) return i;
		return m_size;
	}

private:
#ifdef __BTREE_DEBUG
	void print(int indent) const
	{
		for(size_t i = 0; i < size(); i++)
		{
			for(int j = 0; j < indent; j++)
				printf("  ");
			printf("%d: %d\n", key(i), val(i));
			if (ptr(i) != ptr_t())
				ptr(i)->print(indent+1);
		}
	}

	bool validate(int goal_height, bool is_root) const
	{
		size_t my_min_size = min_size;
		if (is_root && goal_height == 0) my_min_size = 1;
		if (is_root && goal_height != 0) my_min_size = 2;
		if (size() < my_min_size || size() > max_size)
		{
			printf("Size out of range (%d)\n", (int) size());
			return false;
		}
		if (m_height != goal_height)
		{
			printf("Height mismatch (actual %d, goal %d)\n", (int) m_height, (int) goal_height);
			return false;
		}
		for(size_t i = 0; i < size(); i++)
		{
			if (m_height == 0)
			{
				if (ptr(i) != ptr_t())
				{
					printf("Non null down pointer on height zero");
					return false;
				}
			}
			else
			{
				if (ptr(i) == ptr_t())
				{
					printf("NULL down point on non-zero height");
					return false;
				}
				if (!ptr(i)->validate(goal_height - 1, false))
					return false;
				
				if (ptr(i)->key(0) != key(i))
				{
					printf("Down key does not equal first key of down node");
					printf("Down key = %d, it key = %d", ptr(i)->key(0), key(i));
					return false;
				}
				if (ptr(i)->compute_total() != val(i))
				{
					printf("Failure of total data, key = %d, value %d vs %d", 
						ptr(i)->key(0), ptr(i)->compute_total(), val(i));
					return false;
				}
			}
		}
		return true;
	}
#endif

	// Constructor for an empty bnode
	// Used during deserialization
	bnode(Policy& policy, int height) 
		: m_policy(policy)
		, m_height(height)
		, m_size(0)
	{
#ifdef __BTREE_DEBUG 
		g_node_count++;
#endif
	}

private:
	// Find the entry for a key
	size_t find_by_key(const key_t& k)
	{
		size_t i = upper_bound(k);
		if (i != 0) i--;
		return i;
	}

	void assign(size_t i, const ptr_t& newval) { 
		m_keys[i] = newval->key(0);
		m_values[i] = newval->total();
		m_ptrs[i] = newval;
	}

	void copy_entry(size_t i, size_t j)
	{
		m_keys[i] = m_keys[j];
		m_values[i] = m_values[j];
		m_ptrs[i] = m_ptrs[j];
	}

	void insert(const key_t& k, const value_t& v, const ptr_t& down)
	{
		int loc = (int) lower_bound(k);
		for(int i = m_size; i > loc; i--)
			copy_entry(i, i-1);
		m_keys[loc] = k;
		m_values[loc] = v;
		m_ptrs[loc] = down;
		m_size++;
	}
	
	void insert(const ptr_t& down)
	{
		insert(down->key(0), down->total(), down);
	}

	void erase(size_t begin, size_t end)
	{
		int diff = end - begin;
		for(int i = begin; i + diff < (int) m_size; i++)
			copy_entry(i, i+diff);
		for(int i = m_size - diff; i < (int) m_size; i++)
		{
			m_keys[i] = okey_t();
			m_values[i] = ovalue_t();
			m_ptrs[i] = ptr_t();
		}
			
		m_size -= diff;
	}
	void erase(size_t loc) { erase(loc, loc+1); }

	value_t compute_total() const
	{
		// Initialize return value via copy construction
		// Always guarenteed to have 1 element, no need for default construction
		// this way, which helps with implemention of 'min' accumulators
		value_t total = val(0);
		// Do the rest of the entries
		for(size_t i = 1; i < size(); i++)
		{
			m_policy.aggregate(total, val(i));
		}
		return total;
	}

	void recompute_total() 
	{
		m_total = compute_total();
	}


	bnode* maybe_split()
	{
		// If the node isn't too big, and fix up total
		if (size() <= max_size)
		{
			recompute_total();
			return NULL;
		}

		// Create a new bnode with the same height as me
		bnode* r = new bnode(m_policy, m_height);

		// Compute the size of half (rounded down) to keep
		int keep_size = size() / 2;
		// Copy second of the entries into the new node
		for(size_t i = 0; i < size() - keep_size; i++)
		{
			r->m_keys[i] = m_keys[i + keep_size];
			r->m_values[i] = m_values[i + keep_size];
			r->m_ptrs[i] = m_ptrs[i + keep_size];
			r->m_size = size() - keep_size;
		}
		// Erase them from me
		for(size_t i = keep_size; i < size(); i++)
		{
			m_keys[i] = okey_t();
			m_values[i] = ovalue_t();
			m_ptrs[i] = ptr_t();
		}
	
		m_size = keep_size;

		recompute_total();
		r->recompute_total();
		// Return new node
		return r;
	}

	update_result erase_fixup(cache_t& cache, ptr_t& peer_ptr)
	{
		// If we still have a valid number of nodes, we're done
		if (size() >= min_size)
		{
			// Still need to fix total
			recompute_total();
			// Return easy case
			return ur_erase; 
		}	
		// Handle the special root case
		if (peer_ptr == ptr_t())
		{
			// We should only ever run out of entries as the last
			// erase of the entire tree.
			if (size() == 0)
				return ur_empty; 
			// No more recursion, compute totals
			recompute_total();
			// Figure out which enum to return
			if (m_height != 0 && size() == 1)
				return ur_singular; // Down to 1 entry at top of tree
			return ur_erase;
		}
		// We are going to modify peer, let's copy first
		bnode* peer = peer_ptr->copy();
		// Now we try to steal from peer
		if (peer->size() > min_size)
		{
			// Get the appropriate peer entry
			size_t pi = (m_policy.less(peer->key(0), key(0))) 
					? peer->size() - 1 // Last of peer before me
					: 0 // First of peer after me
					;

			// 'Move' the entry over
			insert(peer->key(pi), peer->val(pi), peer->ptr(pi));
			peer->erase(pi);
			// Recompute self and peer's totals
			recompute_total();
			peer->recompute_total();
			// Set output
			peer_ptr = cache.new_node(peer);
			// Return my state
			return ur_steal;
		}
		// Looks like we need to merge with peer
		// Add my entries into it, and recompute total
		// TODO: Make this not slow!
		for(size_t i = 0; i < size(); i++)
			peer->insert(*m_keys[i], *m_values[i], m_ptrs[i]);
		// Fix peers total
		peer->recompute_total();
		// Set output
		peer_ptr = cache.new_node(peer);
		// Return the fact that I merged
		return ur_merge;
	}

	Policy& m_policy;
	int m_height;  // The height of this node (0 = leaf)
	value_t m_total;  // Total of all down entries, cached
	size_t m_size;
	okey_t m_keys[max_size + 1];  // All my keys
	ovalue_t m_values[max_size + 1];  // All my values
	ptr_t m_ptrs[max_size + 1];  // All my pointers
};

}
#endif
