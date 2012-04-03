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

#ifndef __biter_h__
#define __biter_h__

#include "bdecl.h"

template<class Policy>
class biter
{
public:
	typedef typename Policy::key_t key_t;
	typedef typename Policy::value_t value_t;
	typedef typename apply_policy<Policy>::ptr_t ptr_t;

	// Construct a totally empty iterator
	biter()
		: m_height(0)
		, m_pair()
	{}

	// Construct an empty iterator to a tree snapshot
	biter(const ptr_t& root, size_t height) 
		: m_height(height)
		, m_nodes(height)
		, m_iters(height)
		, m_pair()
	{
		if (m_height)
		{
			assert(root != ptr_t());
			m_nodes[0] = root;
			m_iters[0] = root->size();
		}
	}

	~biter() { }

	biter(const biter& rhs)
		: m_height(rhs.m_height)
		, m_nodes(rhs.m_nodes)
		, m_iters(rhs.m_iters)
		, m_pair(rhs.m_pair)
	{}
	
	void operator=(const biter& rhs)
	{
		m_height = rhs.m_height;
		m_nodes = rhs.m_nodes;
		m_iters = rhs.m_iters;
		m_pair = rhs.m_pair;
	}

	bool operator==(const biter& other) const
	{
		// Ignore tree of different size
		if (m_height != other.m_height)
			return false;

		// Empty trees are all the same
		if (m_height == 0)
			return true;

		// Iterators from different trees differ
		if (m_nodes[0] != other.m_nodes[0])
			return false;

		// Handle special cases for 'end'
		if (is_end())
			return other.is_end();
		else if (other.is_end())
			return false;
		
		// Now compare the iterators in depth
		return m_iters == other.m_iters;
	}
	

private:
	template<class Functor>
	void walk_until(const Functor& threshold, value_t& start, const ptr_t& node, size_t& it, size_t end)
	{
		assert(it <= end);
		while(it != end)
		{
			value_t new_val = start;
			new_val += node->val(it);
			if (threshold(new_val))
				break;
			start = new_val;
			++it;
		}
	}

public:
	template<class Functor>
	void accumulate_until(const Functor& threshold, value_t& start, const biter& end)
	{
		// Skip things if we are already at the end
		if (is_end()) return;
		// Assert that we are part of the same tree
		assert(m_nodes[0] == end.m_nodes[0]);

		bool non_end = !(end.is_end());
		
		// Walk forward and go 'up' if we walk off the end of a given level
		int cur = m_height - 1;
		//printf("Walking up\n");
		while(cur >= 0)
		{
			//printf("cur = %d, start = %d, total = %d\n", cur, m_iters[cur]->first, start);
			if (non_end && m_nodes[cur] == end.m_nodes[cur])
				walk_until(threshold, start, m_nodes[cur], m_iters[cur], end.m_iters[cur]);
			else
				walk_until(threshold, start, m_nodes[cur], m_iters[cur], m_nodes[cur]->size());
			//printf("Post walk: total = %d\n", start);

			if (m_iters[cur] != m_nodes[cur]->size())
				break;

			// If we are about to walk off the whole tree, we are at end
			if (cur == 0)
				return;

			cur--;
			m_iters[cur]++;  // Skip 'current' since I handled it already
		}

		// Otherwise, we now want to walk down and move forward
		//printf("Walking down\n");
		while(cur + 1 < (int) m_height)
		{
			m_nodes[cur+1] = m_nodes[cur]->ptr(m_iters[cur]);
			m_iters[cur+1] = 0;
			cur++;
			
			//printf("cur = %d, start = %d, total = %d\n", cur, m_iters[cur]->first, start);
			if (non_end && m_nodes[cur] == end.m_nodes[cur])
				walk_until(threshold, start, m_nodes[cur], m_iters[cur], end.m_iters[cur]);
			else
				walk_until(threshold, start, m_nodes[cur], m_iters[cur], m_nodes[cur]->size());
			//printf("Post walk: total = %d\n", start);
		}
		m_pair.first = m_nodes[m_height-1]->key(m_iters[m_height-1]);
		m_pair.second= m_nodes[m_height-1]->val(m_iters[m_height-1]);
	}

	// Set this iterator to the tree begin
	void set_begin()
	{
		if (m_height == 0) return;
		for(size_t i = 0; i + 1 < m_height; i++)
		{
			m_iters[i] = 0;
			m_nodes[i+1] = m_nodes[i]->ptr(m_iters[i]);	
		}
		m_iters[m_height - 1] = 0;
		m_pair.first = m_nodes[m_height-1]->key(m_iters[m_height-1]);
		m_pair.second= m_nodes[m_height-1]->val(m_iters[m_height-1]);
	}

	void set_rbegin()
	{
		if (m_height == 0) return;
		for(size_t i = 0; i + 1 < m_height; i++)
		{
			m_iters[i] = m_nodes[i]->size() - 1;
			m_nodes[i+1] = m_nodes[i]->ptr(m_iters[i]);	
		}
		m_iters[m_height - 1] = m_nodes[m_height - 1]->size() - 1;
		m_pair.first = m_nodes[m_height-1]->key(m_iters[m_height-1]);
		m_pair.second= m_nodes[m_height-1]->val(m_iters[m_height-1]);
	}

	// Set this iterator to the tree end
	void set_end()
	{
		// Handle empty case
		if (m_height == 0)
			return;
		// Set the top of the stack to end
		m_iters[0] = m_nodes[0]->size();
	}

	void set_find(const key_t& k)
	{
		set_lower_bound(k);
		if (m_pair.first == k)
			return;
		set_end();
	}

	void set_lower_bound(const key_t& k)
	{
		if (m_height == 0)
			return;
		if (!m_nodes[0]->get_policy().less(m_nodes[0]->key(0), k))
		{
			set_begin();
			return;
		}
		for(size_t i = 0; i + 1 < m_height;i++)
		{
			m_iters[i] = m_nodes[i]->lower_bound(k) - 1;
			m_nodes[i+1] = m_nodes[i]->ptr(m_iters[i]);
		}
		m_iters[m_height - 1] = m_nodes[m_height-1]->lower_bound(k);
		m_iters[m_height - 1]--;
		increment();
	}

	void set_upper_bound(const key_t& k)
	{
		if (m_height == 0)
			return;
		if (m_nodes[0]->get_policy().less(k, m_nodes[0]->key(0)))
		{
			set_begin();
			return;
		}
		for(size_t i = 0; i + 1 < m_height;i++)
		{
			m_iters[i] = m_nodes[i]->upper_bound(k) - 1;
			m_nodes[i+1] = m_nodes[i]->ptr(m_iters[i]);
		}
		m_iters[m_height - 1] = m_nodes[m_height-1]->upper_bound(k);
		m_iters[m_height - 1]--;
		increment();
	}

	void increment()
	{
		// Make sure we are not at end
		assert(!is_end());
	
		// Move the lowest level forward one	
		int cur = m_height - 1;
		m_iters[cur]++;
		// If we hit the end at our current level, move up and increment
		while(m_iters[cur] == m_nodes[cur]->size())
		{
			cur--;
			if (cur < 0)
				return;  // If we hit the top, we are done
			m_iters[cur]++;
		}

		cur++;
		while(cur < (int) m_height)
		{
			m_nodes[cur] = m_nodes[cur -1]->ptr(m_iters[cur-1]);	
			m_iters[cur] = 0;
			cur++;
		}
		m_pair.first = m_nodes[m_height-1]->key(m_iters[m_height-1]);
		m_pair.second= m_nodes[m_height-1]->val(m_iters[m_height-1]);
	}

	void decrement()
	{
		// Handle special 'end' case
		if (is_end())
		{
			set_rbegin();
			return;
		}
	
		// Find the lowest level that can move back one
		int cur = m_height - 1;
		while(m_iters[cur] == 0)
		{
			cur--;
			if (cur < 0) 
				return;  // begin()-- is undefined, make it a no-op
		}
		// Move back
		m_iters[cur]--;
		// Set things to 'rbegin' the rest of the way down
		cur++;
		while(cur < (int) m_height)
		{
			m_nodes[cur] = m_nodes[cur-1]->ptr(m_iters[cur-1]);	
			m_iters[cur] = m_nodes[cur]->size();
			m_iters[cur]--;
			cur++;
		}
		m_pair.first = m_nodes[m_height-1]->key(m_iters[m_height-1]);
		m_pair.second= m_nodes[m_height-1]->val(m_iters[m_height-1]);
	}

	bool is_end() const { return m_height == 0 || m_iters[0] == m_nodes[0]->size(); }
	const std::pair<key_t, value_t>& get_pair() const { 
		assert(!is_end()); 
		return m_pair; 
	}
	const key_t& get_key() const { assert(!is_end()); return m_pair.first; }
	const value_t& get_value() const { assert(!is_end()); return m_pair.second; }
	ptr_t get_root() const { return m_nodes[0]; }
	size_t get_height() const { return m_height; }

private:
	// The height of the tree we are attached to
	size_t m_height;  
	// The nodes in the tree
	// nodes[0] = root
	// nodes[i+1] = nodes[i]->ptr(iters[i])
	std::vector<ptr_t> m_nodes;
	// The iterators within each node
	std::vector<size_t> m_iters; 
	std::pair<key_t, value_t> m_pair;
};

#endif
