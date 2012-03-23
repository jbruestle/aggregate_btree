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

void deserialize(const int& nothing, readable& src, int& out)
{
	deserialize(src, out);
}

#include "btree.h"
#include <map>

const size_t k_op_count = 20000;
const size_t k_key_range = 1000;
const size_t k_value_range = 100;
const size_t k_num_snapshots = 5;
const bool noisy = false;

typedef btree<int, int> btree_t;
typedef btree_t::snapshot_t bsnapshot_t;
typedef bsnapshot_t::const_iterator biterator_t;

typedef std::map<int, int> mtree_t;
typedef std::map<int, int> msnapshot_t;
typedef msnapshot_t::const_iterator miterator_t;

typedef bsnapshot_t::value_type value_type;
class mock_snapshot;
class mock_tree;

class mock_iterator : public boost::iterator_facade<
	mock_iterator,
	const value_type,
	boost::bidirectional_traversal_tag> 
{
	friend class boost::iterator_core_access;
	friend class mock_snapshot;
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
		return *m_biter;
	}
	
	biterator_t m_biter;
	miterator_t m_miter;
};

class mock_snapshot 
{
public:
	friend class mock_tree;
	typedef mock_iterator const_iterator;
	mock_snapshot(const bsnapshot_t& bsnap, const msnapshot_t& msnap)
		: m_bsnap(bsnap)
		, m_msnap(msnap)
	{}
	
	const_iterator begin() const { return mock_iterator(m_bsnap.begin(), m_msnap.begin()); }
	const_iterator end() const { return mock_iterator(m_bsnap.end(), m_msnap.end()); }
	const_iterator find(int k) const { return mock_iterator(m_bsnap.find(k), m_msnap.find(k)); }
	const_iterator lower_bound(int k) const { return mock_iterator(m_bsnap.lower_bound(k), m_msnap.lower_bound(k)); }
	const_iterator upper_bound(int k) const { return mock_iterator(m_bsnap.upper_bound(k), m_msnap.upper_bound(k)); }

	template<class Functor>
	void accumulate_until(const_iterator& cur, int& total, const const_iterator& end, const Functor& threshold)
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
		m_bsnap.accumulate_until(bit, val2, end.m_biter, threshold);
		if (val != val2)
		{
			printf("Values differ, %d vs %d\n", val, val2);
			exit(1);
		}
		if ((mit == m_msnap.end()) != (bit == m_bsnap.end()))
		{
			printf("AU: Not both or neither end\n");
			exit(1);
		}
		if (mit != m_msnap.end())
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
	bsnapshot_t m_bsnap;
	msnapshot_t m_msnap;
};

class mock_tree
{
public:
	mock_tree() 
		: m_btree(10, 20)
	{}

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
		off_t lowest = m_btree.lowest_loc();
		if (lowest != std::numeric_limits<off_t>::max())
		{
			m_btree.load_below(lowest);
		}
		return br;
	}

	size_t size() const { return m_mtree.size(); }
	mock_snapshot get_snapshot() const
	{
		return mock_snapshot(m_btree.get_snapshot(), m_mtree);
	}

	void open(const std::string& name, bool create)
	{
		m_btree.open(name, create);
	}

	void close()
	{
		m_btree.close();
	}

	void sync()
	{
		m_btree.sync();
	}
	
	void sync(mock_snapshot& s)
	{
		m_btree.sync(s.m_bsnap);
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

extern size_t g_node_count; 

int main()
{
	printf("Testing btree\n");
	printf("Node count = %d\n", (int) g_node_count);
	mock_tree t;
	std::vector<mock_snapshot> snaps;
	for(size_t i = 0; i < k_num_snapshots; i++)
		snaps.push_back(t.get_snapshot());

	system("rm -r /tmp/lame_tree");
	t.open("/tmp/lame_tree", true);
	for(size_t i = 0; i < k_op_count; i++)
	{
		if (i % 1000 == 0)
		{
			printf("Op %d\n", (int) i);
			if (i % 3000 == 0)
			{
				printf("Node count = %d\n", (int) g_node_count);
				printf("Closing trr\n");
				snaps.clear();
				t.close();
				printf("Node count = %d\n", (int) g_node_count);
				t.open("/tmp/lame_tree", false);
				for(size_t i = 0; i < k_num_snapshots; i++)
					snaps.push_back(t.get_snapshot());
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
			mock_snapshot s = t.get_snapshot();
			mock_iterator it = s.lower_bound(k);
			if (it != s.end())
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
			mock_snapshot& s = snaps[random() % k_num_snapshots];
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
			mock_snapshot& s = snaps[random() % k_num_snapshots];
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
			mock_snapshot& s = snaps[random() % k_num_snapshots];
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
			mock_snapshot& s = snaps[random() % k_num_snapshots];
			mock_iterator it = s.lower_bound(k);
			if (it != s.end())
				k = it->first;
		}
		break;
		case 7:
		{
			int k = random() % k_key_range;
			if (noisy) printf("testing upper_bound at %d\n", k);
			mock_snapshot& s = snaps[random() % k_num_snapshots];
			mock_iterator it = s.upper_bound(k);
			if (it != s.end())
				k = it->first;
		}
		break;
		case 8:
		{
			int k = random() % k_key_range;
			if (noisy) printf("testing find at %d\n", k);		
			mock_snapshot& s = snaps[random() % k_num_snapshots];
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
			mock_snapshot& s = snaps[random() % k_num_snapshots];
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
			snaps[random() % k_num_snapshots] = t.get_snapshot();	
		}
		break;
		}
		if (noisy) t.print();
		if (!t.validate())
			exit(1);
	}
	printf("Node count = %d\n", (int) g_node_count);
	t.sync();
	snaps.clear();
	while(t.size() > 0)
	{
		int val = t.get_snapshot().begin()->first;
		if (noisy) printf("Erasing %d\n", val);
		t.update(val, always_erase());
	}
	t.close();
	printf("Node count = %d\n", (int) g_node_count);
	assert(true);
}
