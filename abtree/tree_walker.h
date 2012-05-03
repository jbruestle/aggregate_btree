
#ifndef __tree_walker_h__
#define __tree_walker_h__

#include "abtree/btree.h"
#include <boost/iterator/iterator_facade.hpp>

template<class tree_type, class functor_type>
class forward_subset_iterator;

template<class tree_type, class functor_type>
class forward_subset_iterator : public boost::iterator_facade<
                forward_subset_iterator<tree_type, functor_type>,
                const typename tree_type::value_type,
                boost::forward_traversal_tag
	>
{
	friend class boost::iterator_core_access;
	typedef typename tree_type::node_ptr_type node_ptr_t;
public:
	typedef typename tree_type::value_type value_type;

	forward_subset_iterator()
		: m_height(0)
		, m_curh(0)
	{}

	forward_subset_iterator(const functor_type& functor, const node_ptr_t& root, size_t height, bool start)
		: m_functor(functor)
		, m_height(height)
		, m_curh(-1)
		, m_ptr(height)
		, m_idx(height)
		, m_value(NULL)
	{
		if (start && height != 0)
		{
			//printf("Generating start\n");
			m_curh = 0;
			m_ptr[0] = root;
			m_idx[0] = 0;
			next_valid();
		}
		if (m_curh != -1)
			m_value = new value_type(m_ptr[m_curh]->key(m_idx[m_curh]), m_ptr[m_curh]->val(m_idx[m_curh]));
	}
private:

	functor_type m_functor;
	int m_height;
	int m_curh;
	std::vector<node_ptr_t> m_ptr;
	std::vector<int> m_idx;
	value_type* m_value;

	void next_valid()
	{
		//printf("next valid\n");
		assert(m_curh != -1);
		assert(m_idx[m_curh] < m_ptr[m_curh]->size());
		while(m_curh != -1)
		{
			// If the current element matches
			//printf("Checking functor on %d\n", m_ptr[m_curh]->val(m_idx[m_curh]));
			if (m_functor(m_ptr[m_curh]->val(m_idx[m_curh]))) 
			{
				//printf("match\n");
				// If we are at the bottrom
				if (m_curh == m_height - 1)
					return;  // Found a valid low level entry
				// Otherwise, go down a level
				//printf("Going down a level\n");
				m_ptr[m_curh + 1] = m_ptr[m_curh]->ptr(m_idx[m_curh]);
				m_idx[m_curh + 1] = 0;
				m_curh++;
			}
			else
			{
				forward_up();
			}
		}
	}
			
	void forward_up()
	{
		// Go forward one
		//printf("forward up: m_curh = %d,m_idx[m_curh]=%d\n", m_curh, m_idx[m_curh]);
		m_idx[m_curh]++;
		// While we are at the end at the current level
		while(m_idx[m_curh] == (int) m_ptr[m_curh]->size())
		{
			// Go up one
			//printf("  forward up: m_curh = %d,m_idx[m_curh]=%d\n", m_curh, m_idx[m_curh]);
			m_ptr[m_curh] = node_ptr_t();
			m_idx[m_curh] = 0;
			m_curh--;
			// If at the top, break
			if (m_curh == -1) break;
			// Go forward at this level
			m_idx[m_curh]++;
		}
	}

	void increment()
	{
		//printf("Incrementing\n");
		forward_up();
		if (m_curh != -1)
			next_valid();
		delete m_value;
		if (m_curh != -1)
			m_value = new value_type(m_ptr[m_curh]->key(m_idx[m_curh]), m_ptr[m_curh]->val(m_idx[m_curh]));
		else
			m_value = NULL;
	}
	bool equal(const forward_subset_iterator& rhs) const
	{
		return m_curh && rhs.m_curh && m_ptr == rhs.m_ptr &&  m_idx == rhs.m_idx;
	}
	const value_type& dereference() const { return *m_value; }
};

template<class tree_type, class functor_type>
std::pair<
	forward_subset_iterator<tree_type, functor_type>, 
	forward_subset_iterator<tree_type, functor_type>
>
forward_subset(const tree_type& tree, const functor_type& functor)
{
	return std::make_pair(
		forward_subset_iterator<tree_type, functor_type>(functor, tree.get_root(), tree.get_height(), true),
		forward_subset_iterator<tree_type, functor_type>(functor, tree.get_root(), tree.get_height(), false)
	);
}

#endif
