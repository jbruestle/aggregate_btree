
#include "abtree/abtree.h"
#include "abtree/tree_walker.h"
#include "gtest/gtest.h"
#include <boost/foreach.hpp>
#define foreach BOOST_FOREACH

class is_greater
{
public:
	is_greater(int min) : m_min(min) {}
	bool operator()(int x) { return x > m_min; }
private:
	int m_min;
};

class max_int
{
public:
	void operator()(int& a, const int& b) const { a = std::max(a,b); }
};

TEST(walker, basic)
{
	typedef abtree<int, int, max_int> tree_type;
	tree_type tree;
	typedef std::map<int, int> map_type;
	map_type map;
	printf("Doing inserts\n");
	for(size_t i = 0; i < 1000; i++)
	{
		int k = random()%1000;
		int v = random()%1000;
		tree.insert(std::make_pair(k,v));
		map.insert(std::make_pair(k,v));
	}
	printf("Doing walk\n");
	is_greater f(700);
	map_type::iterator it = map.begin();
	while(it != map.end() && !f(it->second)) ++it;
	foreach(const tree_type::value_type& kvp, forward_subset(tree, f))
	{
		printf("(%d, %d) vs (%d, %d)\n", kvp.first, kvp.second, it->first, it->second);	
		assert(it != map.end());
		assert(kvp.first == it->first);
		assert(kvp.second == it->second);
		++it;
		while(it != map.end() && !f(it->second)) ++it;
	}
	printf("Done\n");
}
