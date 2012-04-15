
#include "gtest/gtest.h"
#include "abtree/interval_tree.h"
#include <set>
#include <boost/foreach.hpp>
#include <boost/iterator/iterator_facade.hpp>
#define foreach BOOST_FOREACH

class test_element
{
public:
	test_element(int start, int end, const std::string& name)
		: m_start(start)
		, m_end(end)
		, m_name(name)
	{}

	typedef int position_t;
	int get_start() const { return m_start; }
	int get_end() const { return m_end; }
	const std::string& get_name() const { return m_name; }
	bool operator<(const test_element& rhs) const {
		if (m_start < rhs.m_start) return true;
		if (rhs.m_start < m_start) return false;
		if (m_end < rhs.m_end) return true;
		if (rhs.m_end < m_end) return false;
		return m_name < rhs.m_name;
	}
	bool operator==(test_element const& rhs) const {
		return (m_start == rhs.m_start && m_end == rhs.m_end && m_name == rhs.m_name);
	}
	std::string to_string() const { return printstring("(%d, %d):%s", m_start, m_end, m_name.c_str()); }

private:
	int m_start;
	int m_end;
	std::string m_name;
};

template<class value_t, class position_t, class IntervalGet = default_interval_functor<value_t, position_t> >
class fake_interval_tree
{
	typedef std::pair<position_t, value_t> element_t;
	typedef std::set<element_t> set_t;
	typedef IntervalGet interval_functor_type;
public:
	class const_iterator : public boost::iterator_facade<
		const_iterator,
		const value_t,
		boost::forward_traversal_tag
	>
	{
		friend class boost::iterator_core_access;
		friend class fake_interval_tree;
		typedef typename set_t::const_iterator inner_iterator;
	public:
		const_iterator() {}
	private:
		const_iterator(const inner_iterator& it) : m_it(it) {}
		void increment() { m_it++; }
		void decrement() { m_it--; }
		bool equal(const_iterator const& other) const { return m_it == other.m_it; }
		const value_t& dereference() const { return m_it->second; }
                typename set_t::const_iterator m_it;
        };

	class overlap_iterator : public boost::iterator_facade<
		overlap_iterator,
		const value_t,
		boost::forward_traversal_tag
	>
	{
		friend class boost::iterator_core_access;
		friend class fake_interval_tree;
	public:
		overlap_iterator() : m_tree(NULL), m_start(), m_end() {}

	private:
		overlap_iterator(const fake_interval_tree* tree, const position_t& start, const position_t& end, bool is_end)
			: m_tree(tree)
			, m_start(start)
			, m_end(end)
		{
			if (is_end) 
			{
				m_it = m_tree->m_set.end();
				return;
			}
			m_it = m_tree->m_set.begin();
			skip_forward();
		}

		void skip_forward()
		{
			while(m_it != m_tree->m_set.end() && !in_range()) ++m_it;
		}

		bool in_range() 
		{
			return m_tree->get_start(m_it->second) < m_end
				&& m_start < m_tree->get_end(m_it->second);
		}
		void increment() { ++m_it; skip_forward(); }
		bool equal(overlap_iterator const& other) const { return m_it == other.m_it; }
		const value_t& dereference() const { return m_it->second; }

		const fake_interval_tree* m_tree;
		position_t m_start;
		position_t m_end;
		typename set_t::const_iterator m_it;
	};
	typedef std::pair<overlap_iterator, overlap_iterator> range_t;
	
	fake_interval_tree() {}
	void insert(const value_t& val) { m_set.insert(to_element(val)); }
	void erase(const value_t& val) { m_set.erase(to_element(val)); }
	void erase(const const_iterator& it) { m_set.erase(it.m_it); }
	void erase(const overlap_iterator& it) { m_set.erase(it.m_it); }
	const_iterator begin() const { return const_iterator(m_set.begin()); }
	const_iterator end() const { return const_iterator(m_set.end()); }
	const_iterator find(const value_t& val) const { return const_iterator(m_set.find(to_element(val))); }
	range_t find(const position_t& start, const position_t& end) const
	{
		return std::make_pair(overlap_iterator(this, start, end, false), overlap_iterator(this, start, end, true));
	}
	size_t size() const { return m_set.size(); }
	
private:
	position_t get_start(const value_t& v) const { return m_interval_functor(v).first; }
        position_t get_end(const value_t& v) const { return m_interval_functor(v).first; }

	element_t to_element(const value_t& val) const { return std::make_pair(get_start(val), val); }
	set_t m_set;
	interval_functor_type m_interval_functor;
};

template<class it1_t, class it2_t>
class cmp_iter : public boost::iterator_facade<
	cmp_iter<it1_t, it2_t>,
	const typename it1_t::value_type,
	boost::bidirectional_traversal_tag
>       
{
	friend class boost::iterator_core_access;
public:
	cmp_iter() {}
	cmp_iter(const it1_t& it1, const it2_t& it2) : m_it1(it1), m_it2(it2) {}
private:
	void increment() { m_it1++; m_it2++; }
	void decrement() { m_it1--; m_it2--; }
	bool equal(cmp_iter const& other) const 
	{
		bool r1 = (m_it1 == other.m_it1);
		bool r2 = (m_it2 == other.m_it2);
		assert(r1 == r2);
		return r1;
	}
	const typename it1_t::value_type& dereference() const 
	{
		const typename it1_t::value_type& v1 = *m_it1;
		const typename it2_t::value_type& v2 = *m_it2;
		assert(v1 == v2);
		return v1;
	}
public:
	it1_t m_it1;
	it2_t m_it2;
};

template<class value_t, class position_t>
class check_interval_tree
{
	typedef interval_tree<value_t, position_t> iv_t;
	typedef fake_interval_tree<value_t, position_t> fiv_t;
public:
	typedef cmp_iter<typename iv_t::const_iterator, typename fiv_t::const_iterator> const_iterator; 
	typedef cmp_iter<typename iv_t::overlap_iterator, typename fiv_t::overlap_iterator> overlap_iterator; 
	typedef std::pair<overlap_iterator, overlap_iterator> range_t;

	check_interval_tree() {}
	void insert(const value_t& val) { m_iv.insert(val); m_fiv.insert(val); }
	void erase(const value_t& val) { m_iv.erase(val); m_fiv.erase(val); }
	void erase(const const_iterator& it) { m_iv.erase(it.m_it1); m_fiv.erase(it.m_it2); }
	void erase(const overlap_iterator& it) { m_iv.erase(it.m_it1); m_fiv.erase(it.m_it2); }
	const_iterator begin() const { return const_iterator(m_iv.begin(), m_fiv.begin()); }
	const_iterator end() const { return const_iterator(m_iv.end(), m_fiv.end()); }
	const_iterator find(const value_t& val) const { return const_iterator(m_iv.find(val), m_fiv.find(val)); }
	range_t find(const position_t& start, const position_t& end) const
	{
		typename iv_t::range_t r1 = m_iv.find(start, end); 	
		typename fiv_t::range_t r2 = m_fiv.find(start, end); 	
		return std::make_pair(overlap_iterator(r1.first, r2.first), overlap_iterator(r1.second, r2.second));
	}
	size_t size() const 
	{
		size_t s1 = m_iv.size(); 
		size_t s2 = m_fiv.size();  
		assert(s1 == s2);
		return s1;
	}
	
private:
	iv_t m_iv;
	fiv_t m_fiv;
};

void generate_random_region(int& start, int& end)
{
	start = random() % 10000;
	int len = (random() % 12) * (random() % 12) * (random() % 12);
	if (len > 5)
		len += random() % (len/2);
	end = start + len;
}

TEST(interval_tree, basic)
{
	typedef check_interval_tree<test_element, int> tree_t;
	tree_t tree;
	int start, end;
	int rr = 0;
	int xx = 0;
	for(size_t i = 0; i < 10000; i++)
	{
		switch(random() % 4)
		{
			case 0:
			case 1:
			{
				// Do an insert
				generate_random_region(start, end);
				test_element te(start, end, printstring("object%d", i));
				tree.insert(te);
			}
			break;
			case 2:
			{
				// Test iterator, basic find, and erase
				if (tree.size() == 0) continue;
				size_t off = random() % tree.size();
				tree_t::const_iterator it = tree.begin();
				for(size_t j = 0; j < off; j++) 
					it++;
				const test_element& te = *it;
				tree_t::const_iterator it2 = tree.find(te);
				assert(it == it2);
				tree.erase(it2);
			}
			break;
			case 3:
			{
				// Test range search
				generate_random_region(start, end);
				tree_t::range_t r = tree.find(start, end);	
				foreach(const test_element& te, r)
				{
					rr++;
					xx += te.get_start();
				}
			}
			break;
		}
	}
	printf("Total size results = %d\n", (int) tree.size());
	printf("Total range results = %d, meaningless number = %d\n", rr, xx);
}	
	
				
