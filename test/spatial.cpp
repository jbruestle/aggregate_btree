
#include "abtree/abtree.h"
#include "abtree/spatial.h"
#include "abtree/tree_walker.h"
#include "gtest/gtest.h"
#include <boost/foreach.hpp>
#include <set>
#define foreach BOOST_FOREACH

typedef point<2> point_t;
typedef bounding_region<2> bounding_t;

int check_count = 0;

class is_inside
{
public:
	is_inside(const bounding_t& bounds) : m_bounds(bounds) {}
	bool operator()(const point_t& p) 
	{
		check_count++;
		for(size_t i = 0; i < 2; i++)
			if (p.coords[i] > m_bounds.max_point.coords[i] || p.coords[i] < m_bounds.min_point.coords[i])
				return false;
		return true;
	}
	bool operator()(const bounding_t& b) 
	{
		check_count++;
		for(size_t i = 0; i < 2; i++)
			if (b.min_point.coords[i] > m_bounds.max_point.coords[i] || b.max_point.coords[i] < m_bounds.min_point.coords[i])
				return false;
		return true;
	}
private:
	bounding_t m_bounds;
};

struct dumb_cmp
{
	bool operator()(const point_t& a, const point_t& b) const
	{ 
		if (a.coords[0] != b.coords[0]) return a.coords[0] < b.coords[0];
		return a.coords[1] < b.coords[1];
	}
};

TEST(spatial, basic)
{
        typedef abtree<point_t, bounding_t, spatial_accumulate, spatial_less> bt_t;
        typedef std::vector<point_t> vec_t;
	bt_t abtree;
	vec_t vec;
	printf("Adding 100000 nodes\n");
	for(size_t i = 0; i < 100000; i++)
	{
		point_t p;
		p.coords[0] = random() % 200000 - 100000;
		p.coords[1] = random() % 200000 - 100000;
		bounding_t bound;
		bound.min_point = p;
		bound.max_point = p;
		abtree.insert(std::make_pair(p, bound));
		vec.push_back(p);
		
	}
	for(size_t i = 0; i < 100; i++)
	{
		point_t p;
		p.coords[0] = random() % 200000 - 100000;
		p.coords[1] = random() % 200000 - 100000;
		bounding_t bounds;
		bounds.min_point = p;
		bounds.max_point = p;
		bounds.max_point.coords[0] += random() % 3000;
		bounds.max_point.coords[1] += random() % 3000;
		is_inside check_it(bounds);
		std::set<point_t, dumb_cmp> simple;
		std::set<point_t, dumb_cmp> tricky;
		for(size_t j = 0; j < 100000; j++)
		{
			if (check_it(vec[j]))
				simple.insert(vec[j]);
		}
		check_count = 0;
		foreach(const bt_t::value_type& kvp, forward_subset(abtree, check_it))
			tricky.insert(kvp.first);
		ASSERT_EQ(simple.size(), tricky.size());
		std::set<point_t, dumb_cmp>::iterator it1, it2;
		it1 = simple.begin(); it2 = tricky.begin();
		while(it1 != simple.end())
		{
			ASSERT_EQ(it1->coords[0], it2->coords[0]);
			ASSERT_EQ(it1->coords[1], it2->coords[1]);
			it1++; it2++;
		} 
		ASSERT_LT(check_count, 200);
		printf("Case %d, %d nodes found, %d nodes checked\n", (int) i, (int) simple.size(), check_count);
	}
}
