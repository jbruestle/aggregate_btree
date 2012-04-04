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

#include "serial.h"
#include "file_bstore.h"

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

#define __BTREE_DEBUG
#include "btree.h"
#include <map>

const size_t k_op_count = 10000;
const size_t k_key_range = 1000;
const size_t k_value_range = 100;
const size_t k_num_snapshots = 5;
const bool noisy = false;

typedef disk_btree<my_policy> btree_t;
typedef btree_t::const_iterator biterator_t;
typedef btree_t::store_type store_t;

typedef std::map<int, int> mtree_t;
typedef mtree_t::const_iterator miterator_t;

typedef mtree_t::value_type value_type;
class mock_tree;

class mock_iterator : public boost::iterator_facade<
	mock_iterator,
	const value_type,
	boost::bidirectional_traversal_tag> 
{
	friend class boost::iterator_core_access;
	friend class mock_tree;
public:
	mock_iterator() {}
	mock_iterator(biterator_t biter, miterator_t miter)
		: m_biter(biter)
		, m_miter(miter)
	{}

private:
	void increment() { m_biter++; m_miter++; }
	void decrement() { m_biter--; m_miter--; }
	bool equal(mock_iterator const& other) const
	{
		bool br = (m_biter == other.m_biter);
		bool mr = (m_miter == other.m_miter);
		if (br != mr)
		{
			printf("FAILED in equal %d vs %d\n", br, mr);
			exit(1);
		}
		return br;
	}
	const value_type& dereference() const
	{
		bool eq = (m_biter->first == m_miter->first && m_biter->second == m_miter->second);
		if (!eq)
		{
			printf("FAILED in deref %d:%d vs %d:%d\n", m_biter->first,m_biter->second,m_miter->first,m_miter->second);
			exit(1);
		}
		const value_type& r = *m_biter;
		return r;
	}
	
	biterator_t m_biter;
	miterator_t m_miter;
};

class mock_tree
{
public:
	mock_tree(store_t& store, const std::string& name, const mtree_t& mt) 
		: m_btree(store, name)
		, m_mtree(mt)
	{}

	mock_tree(const mock_tree& rhs)
		: m_btree(rhs.m_btree)
		, m_mtree(rhs.m_mtree)
	{}

	void operator=(const mock_tree& rhs)
	{
		m_btree = rhs.m_btree;
		m_mtree = rhs.m_mtree;
	}
	
	const mtree_t& get_mtree() { return m_mtree; }

	template<class Updater>
	bool update(int k, const Updater& updater)
	{
		bool br = m_btree.update(k, updater);
		bool mr;
		mtree_t::iterator it = m_mtree.find(k);
		if (it == m_mtree.end())
		{
			bool exists = false;
			int newval;
			mr = updater(newval, exists);
			if (mr && exists)
			{
				m_mtree.insert(std::make_pair(k, newval));
			}
		}
		else
		{
			bool exists = true;
			mr = updater(it->second, exists);
			if (mr && !exists)
				m_mtree.erase(it);
		}
		assert(br == mr);
		return br;
	}

	void clear() { m_btree.clear(); m_mtree.clear(); }
	size_t size() const { 
		size_t s1 = m_mtree.size();
		size_t s2 = m_btree.size();
		assert(s1 == s2);
		return s1; 
	}

	void sync(const std::string& name)
	{
		m_btree.sync(name);
	}
	
	void print()
	{
		m_btree.print();
		/*
		mtree_t::const_iterator it, itEnd = m_mtree.end();
		for(it = m_mtree.begin(); it != itEnd; ++it)
		{
			printf("(%d, %d)\n", it->first, it->second);
		}
		*/
	}

	bool validate()
	{
		return m_btree.validate();
	}

	mock_iterator begin() const { return mock_iterator(m_btree.begin(), m_mtree.begin()); }
	mock_iterator end() const { return mock_iterator(m_btree.end(), m_mtree.end()); }
	mock_iterator find(int k) const { return mock_iterator(m_btree.find(k), m_mtree.find(k)); }
	mock_iterator lower_bound(int k) const { return mock_iterator(m_btree.lower_bound(k), m_mtree.lower_bound(k)); }
	mock_iterator upper_bound(int k) const { return mock_iterator(m_btree.upper_bound(k), m_mtree.upper_bound(k)); }

	template<class Functor>
	void accumulate_until(mock_iterator& cur, int& total, const mock_iterator& end, const Functor& threshold)
	{
		miterator_t mit = cur.m_miter;
		int val = total;
		while(mit != end.m_miter)
		{
			if (threshold(val + mit->second))
				break;
			val += mit->second;
			mit++;
		}
		int val2 = total;
		biterator_t bit = cur.m_biter;
		m_btree.accumulate_until(bit, val2, end.m_biter, threshold);
		if (val != val2)
		{
			printf("Values differ, %d vs %d\n", val, val2);
			exit(1);
		}
		if ((mit == m_mtree.end()) != (bit == m_btree.end()))
		{
			printf("AU: Not both or neither end\n");
			exit(1);
		}
		if (mit != m_mtree.end())
		{
			if (mit->first != bit->first || mit->second != bit->second)
			{
				printf("Mismatch on iterator final values, k1 = %d, k2 = %d\n", mit->first, bit->first);
				exit(1);
			}
		}
		total = val;
		cur = mock_iterator(bit, mit);
	}
private:
	btree_t m_btree;
	mtree_t m_mtree;
};

class always_update
{
public:
	always_update(int v) : m_value(v) {}
	bool operator()(int& out, bool& exists) const { out = m_value; exists = true; return true; }
private:
	int m_value;
};

class always_erase
{
public:
	always_erase() {}
	bool operator()(int& what, bool& exists) const { exists = false; return true; }
};

class forever_functor
{
public:
	forever_functor() {}
	bool operator()(const int& total) const { return false; }
};

class until_bigger 
{
public:
	until_bigger(int big) : m_big(big) {}
	bool operator()(const int& total) const { return total >= m_big; }
private:
	int m_big;
};

extern size_t btree_impl::g_node_count; 

int test_disk()
{
	printf("Testing btree\n");
	printf("Node count = %d\n", (int) btree_impl::g_node_count);
	system("rm -r /tmp/lame_tree");
	store_t* store = new store_t(10, 20);
	store->open("/tmp/lame_tree", true);
	mock_tree t(*store, "root", mtree_t());
	std::vector<mock_tree> snaps;
	for(size_t i = 0; i < k_num_snapshots; i++)
		snaps.push_back(t);

	for(size_t i = 0; i < k_op_count; i++)
	{
		if (i % 1000 == 0)
		{
			printf("Op %d\n", (int) i);
			if (i % 3000 == 0)
			{
				snaps.clear();
				printf("Node count = %d\n", (int) btree_impl::g_node_count);
				printf("Closing trr\n");
				t.sync("root");
				mtree_t save = t.get_mtree();
				t.clear();
				delete store;
				printf("Node count = %d\n", (int) btree_impl::g_node_count);
				store = new store_t(10, 20);
				store->open("/tmp/lame_tree", false);
				t = mock_tree(*store, "root", save);
				for(size_t i = 0; i < k_num_snapshots; i++)
					snaps.push_back(t);
			}
		}
		switch(random() % 11)
		{
		case 0:
		case 1:
		{
			int k = random() % k_key_range;
			int v = random() % k_value_range;
			if (noisy) printf("Doing insert of %d\n", k);
			t.update(k, always_update(v));
		}
		break;
		case 2:
		{
			int k = random() % k_key_range;
			if (noisy) printf("Finding near %d\n", k);
			mock_iterator it = t.lower_bound(k);
			if (it != t.end())
			{		
				// TODO: Find an actual erasable element
				if (noisy) printf("Near %d, Erasing %d\n", k, it->first);
				t.update(it->first, always_erase());
			}
		}
		break;
		case 3:
		{
			int k1 = random() % k_key_range;
			int k2 = random() % k_key_range;
			if (k2 < k1)
				std::swap(k1, k2);
			if (noisy) printf("Checking range (%d-%d)\n", k1, k2);
			mock_tree& s = snaps[random() % k_num_snapshots];
			mock_iterator it = s.lower_bound(k1);
			mock_iterator itEnd = s.lower_bound(k2);
			if (it == s.end())
				break;
			int total = 0;
			s.accumulate_until(it, total, itEnd, forever_functor());
			if (noisy) printf("total = %d\n", total);
		}
		break;
		case 4:
		{
			int total;
			if (noisy) printf("looping forward\n");
			mock_iterator it;
			mock_tree& s = snaps[random() % k_num_snapshots];
			for(it = s.begin(); it != s.end(); ++it)
			{
				total += it->second;
				//if (noisy) printf("At %d, %d\n", it->first, it->second);
			}
			//if (noisy) printf("\n");
		}
		break;
		case 5:
		{
			int total;
			if (noisy) printf("looping backword\n");
			mock_tree& s = snaps[random() % k_num_snapshots];
			if (s.begin() == s.end())
				break;
			mock_iterator it = s.end(); --it;
			for(; it != s.begin(); --it)
			{
				total += it->second;
				//if (noisy) printf("At %d, %d\n", it->first, it->second);
			}
			//if (noisy) printf("At %d, %d\n", it->first, it->second);
			total += it->second;
			//if (noisy) printf("\n");
		}
		break;
		case 6:
		{
			int k = random() % k_key_range;
			if (noisy) printf("testing lower_bound at %d\n", k);		
			mock_tree& s = snaps[random() % k_num_snapshots];
			mock_iterator it = s.lower_bound(k);
			if (it != s.end())
				k = it->first;
		}
		break;
		case 7:
		{
			int k = random() % k_key_range;
			if (noisy) printf("testing upper_bound at %d\n", k);
			mock_tree& s = snaps[random() % k_num_snapshots];
			mock_iterator it = s.upper_bound(k);
			if (it != s.end())
				k = it->first;
		}
		break;
		case 8:
		{
			int k = random() % k_key_range;
			if (noisy) printf("testing find at %d\n", k);		
			mock_tree& s = snaps[random() % k_num_snapshots];
			mock_iterator it = s.find(k);
			if (it != s.end())
				k = it->first;
		}
		break;
		case 9:
		{
			int k1 = random() % k_key_range;
			int val_tot = (random() % (k_value_range * 20));
			while (val_tot < 100000000 && random() % 3 == 1)
				val_tot *= 10;
			if (noisy) printf("Checking from %d until tot >= %d\n", k1, val_tot);
			mock_tree& s = snaps[random() % k_num_snapshots];
			mock_iterator it = s.lower_bound(k1);
			if (it == s.end())
				break;
			int total = 0;
			s.accumulate_until(it, total, s.end(), until_bigger(val_tot));
			if (noisy) printf("total = %d\n", total);
			if (it != s.end())
			{
				if (noisy) printf("Total next = %d\n", total + it->second);
			}
		}
		break;
		case 10:
		{
			if (noisy) printf("Copying snapshot\n");
			snaps[random() % k_num_snapshots] = t;	
		}
		break;
		}
		if (noisy) t.print();
		t.size();
		if (!t.validate())
			exit(1);
	}
	printf("Node count = %d\n", (int) btree_impl::g_node_count);
	snaps.clear();
	while(t.size() > 0)
	{
		int val = t.begin()->first;
		if (noisy) printf("Erasing %d\n", val);
		t.update(val, always_erase());
	}
	t.sync("root");
	t.clear();
	delete store;
	printf("Node count = %d\n", (int) btree_impl::g_node_count);
	assert(true);
}

void test_in_memory()
{
	typedef memory_btree<int, int> bt_t;
	typedef bt_t::iterator it_t;
	bt_t tree;
	for(size_t i = 0; i < 100; i++)
	{
		int k = random() % 1000;
		int v = random() % 100;
		tree[k] = v;
		//tree.update(k, always_update(v));
	}
	bt_t tree2 = tree;
	tree = tree2;
	it_t it = tree.begin();
	it_t it_end = tree.end();
	int total = 0;
	tree.accumulate_until(it, total, it_end, forever_functor());
	printf("Total = %d\n", total);
	it = tree.begin();
	int my_total = 0;
	for(; it != it_end; ++it)
	{
		//printf("(%d, %d)\n", it->first, it->second);
		my_total += it->second;
	}
	tree.clear();
	printf("Computed total = %d\n", my_total);
	assert(my_total = total);
	total = 0;
	it = tree2.begin();
	tree2.accumulate_until(it, total, tree2.end(), forever_functor());
	printf("Saved computed total = %d\n", my_total);
	assert(my_total = total);
	tree2 = tree;
	it = tree2.begin();
	total = 0;
	tree2.accumulate_until(it, total, tree2.end(), forever_functor());
	printf("Saved computed total = %d\n", total);
	assert(total == 0);
}

int main()
{
	test_in_memory();
	test_disk();
}
