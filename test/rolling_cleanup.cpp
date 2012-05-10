
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

typedef abtree_store<my_policy> store_t;
typedef store_t::tree_type btree_t;
typedef store_t::tree_ptr_t btree_ptr_t;
typedef btree_t::const_iterator biterator_t;


TEST(rolling, main)
{
	system("rm -rf /tmp/fat_tree");
	store_t store("/tmp/fat_tree", true, 100, 200);
	btree_ptr_t tree_ptr = store.attach("root");
	btree_t& tree = *tree_ptr;
	for(size_t i = 0; i < 1000; i++)
		tree[random() % 1000] = random() % 1000;
	store.mark();
	store.sync();
	//btree_t copy = tree;
	for(size_t i = 0; i < 10000; i++)
	{
		for(size_t j = 0; j < 20; j++)
			tree[random() % 1000] = random() % 1000;
		store.mark();
		store.sync();
	}
}
