
#include "gtest/gtest.h"
#include "abtree/disk_abtree.h"

struct my_policy
{
        typedef int key_type;
        typedef int mapped_type;
        static const size_t node_size = 20;
        bool less(const int& a, const int& b) const { return a < b; }
        void aggregate(int& out, const int& in) const { out += in; }
        void serialize(writable& out, const int& k, const int& v) const { ::serialize(out, k); ::serialize(out, v);}
        void deserialize(readable& in, int& k, int& v) const { ::deserialize(in, k); ::deserialize(in, v); }
};

typedef disk_abtree<my_policy> btree_t;
typedef btree_t::const_iterator biterator_t;
typedef btree_t::store_type store_t;


TEST(rolling, main)
{
	store_t store(100, 200);
	system("rm -rf /tmp/fat_tree");
	store.open("/tmp/fat_tree", true);
	btree_t tree(store, "root");
	for(size_t i = 0; i < 100; i++)
		tree[random() % 1000] = random() % 1000;
	tree.sync("root");
	btree_t copy = tree;
	for(size_t i = 0; i < 1000; i++)
	{
		for(size_t j = 0; j < 20; j++)
			tree[random() % 1000] = random() % 1000;
		tree.sync("root");	
	}
}
