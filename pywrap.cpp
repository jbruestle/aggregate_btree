
#include <boost/python.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
using namespace boost::python;

#include "abtree/disk_abtree.h"

struct obj_count
{
	obj_count() 
		: count(1)
	{}
	obj_count(size_t _count)
		: count(_count)
	{}
	obj_count(const object& _obj) 
		: obj(_obj)
		, count(1)
	{}

	object obj;
	size_t count;
};

class pytree_policy
{
public:
	static const size_t node_size = 20;
        typedef object key_type;
        typedef obj_count mapped_type;

	pytree_policy()
	{}

	pytree_policy(object policy) 
		: m_policy(policy)
	{}

	bool less(const object& a, const object& b) const 
	{
		return extract<int>(m_policy["cmp"](a, b)) < 0; 
	}

	void aggregate(obj_count& out, const obj_count& in) const 
	{
		out.count += in.count;
		out.obj = m_policy["aggregate"](out.obj, in.obj); 
	}

	void serialize(writable& out, const object& k, const obj_count& v) const 
	{
		::serialize(out, extract<std::string>(m_policy["serialize"](k)));
		::serialize(out, v.count);
		::serialize(out, extract<std::string>(m_policy["serialize"](v.obj)));
	}
	void deserialize(readable& in, object& k, obj_count& v) const 
	{ 
		std::string tmp;
		::deserialize(in, tmp); 
		k = m_policy["deserialize"](tmp);
		::deserialize(in, v.count); 
		::deserialize(in, tmp); 
		v.obj = m_policy["deserialize"](tmp);
	}
private:
	object m_policy;
};

/*
class update_functor
{
public:
	update_functor(const object& updater)
		: m_updater(updater)
	{}

	bool operator()(python_kv& io, bool& exists) const
	{
		try
		{
			object new_obj = m_updater(io.m_self);
			if (new_obj == object())
				exists = false;
			else
			{
				exists = true;
				io.m_self = new_obj;
			}
			return true;
		}
		catch(const error_already_set& e)
		{
			return false;
		}
	}

private:
	object m_updater;
};
*/

typedef disk_abtree<pytree_policy> tree_t;
typedef tree_t::store_type store_t;
typedef tree_t::const_iterator iterator_t;

class py_item_iterator
{
public:
	py_item_iterator(const iterator_t& begin, const iterator_t& end) 
		: m_it(begin)
		, m_end(end)
	{}

	py_item_iterator __iter__()
	{
		return *this;
	}

	object next()
	{
		if (m_it == m_end)
		{
			PyErr_SetString(PyExc_StopIteration,"done iterating");
			throw boost::python::error_already_set();
		}
		object r =m_it->first;
		m_it++;
		return r; 
	}

private:
	iterator_t m_it;
	iterator_t m_end;
};

class py_tree;

class py_store : public boost::enable_shared_from_this<py_store>
{
public:
	py_store(const std::string& name, bool create, size_t max_unwritten, size_t max_lru, const object& policy)
		: m_store(name, create, max_unwritten, max_lru, pytree_policy(policy))
	{}

	store_t& get_store() { return m_store; }
	boost::shared_ptr<py_tree> load(const std::string& name, const object& policy);
	void save(const std::string& name, boost::shared_ptr<py_tree> tree);
	void sync()
	{
		m_store.sync();
	}

private:
	store_t m_store;
};

class index_functor
{
public:
	index_functor(size_t until) : m_until(until) {}
	bool operator()(const obj_count& o) const
	{
		return o.count > m_until;
	}
private:
	size_t m_until;
};

class py_tree 
{
public:
	py_tree(const boost::shared_ptr<py_store>& store, const tree_t& tree) 
		: m_store(store)
		, m_tree(tree) 
	{}

	py_tree(const boost::shared_ptr<py_store>& store, const object& policy)
		: m_store(store)
		, m_tree(store->get_store(), pytree_policy(policy))
	{}

	const tree_t& get_tree() { return m_tree; }

	object getitem(const object& key) 
	{ 
		object r;
		iterator_t it = m_tree.find(key);
		if (it == m_tree.end())
		{
			PyErr_SetString(PyExc_KeyError, "no such key");
			throw boost::python::error_already_set();
		}
		return it->second.obj;
	}

	void setitem(const object& key, const object& value) 
	{ 
		m_tree[key] = value;
	}

	void delitem(const object& key)
	{
		m_tree.erase(key);
	}

	py_item_iterator iter(const object& start, const object& end)
	{
		range_t range = make_range(start, end);
		return py_item_iterator(range.first, range.second);
	}

	object total(const object& start, const object& end, const object& base)
	{
		range_t range = make_range(start, end);
		return m_tree.total(range.first, range.second, base).obj;
	}
	
	size_t len(const object& start, const object& end)
	{
		obj_count base(0);
		range_t range = make_range(start, end);
		return m_tree.total(range.first, range.second, base).count;
	}

	object key_at_index(const object& start, const object& end, off_t index)
	{
		if (index < 0) throw_index_error();  // No negative indexes
		range_t range = make_range(start, end);
		if (range.first == range.second) 
			throw_index_error(); // If no range to walk over, give up early
		if (index == 0) 
			return range.first->first;  // If we start, return early to avoid null constructed obj_count
		obj_count total = range.first->second;  // Get first value for accumulator
		range.first++;  // Skip over it
		m_tree.accumulate_until(range.first, total, range.second, index_functor(index));  // Do main walk
		if (range.first == range.second)
			 throw_index_error(); // If I ran out of elements, fail
		return range.first->first; // Otherwise, return key
	}

	boost::shared_ptr<py_tree> copy()
	{
		return boost::shared_ptr<py_tree>(new py_tree(*this));		
	}

private:
	void throw_index_error()
	{
		PyErr_SetString(PyExc_IndexError, "index out of range");
		throw boost::python::error_already_set();
	}
	typedef std::pair<iterator_t, iterator_t> range_t;
	range_t make_range(const object& start, const object& end)
	{
		if (start == object())
		{
			if (end == object())
				return std::make_pair(m_tree.begin(), m_tree.end());
			else
				return std::make_pair(m_tree.begin(), m_tree.lower_bound(end));
		}
		else
		{
			if (end == object())
				return std::make_pair(m_tree.lower_bound(start), m_tree.end());
			else
				return std::make_pair(m_tree.lower_bound(start), m_tree.lower_bound(end));
		}
	}
	py_tree(const py_tree& rhs)
		: m_store(rhs.m_store)
		, m_tree(rhs.m_tree)
	{}

	boost::shared_ptr<py_store> m_store;
	tree_t m_tree;
};

boost::shared_ptr<py_tree> py_store::load(const std::string& name, const object& policy)
{
	boost::shared_ptr<py_tree> ptr(new py_tree(shared_from_this(), m_store.load(name, pytree_policy(policy))));
	return ptr;
}

void py_store::save(const std::string& name, boost::shared_ptr<py_tree> tree)
{
	m_store.save(name, tree->get_tree());
}

#define DEF_ITER(pyname, iter_type) class_<iter_type>(pyname, no_init) \
	.def("__iter__", &iter_type::__iter__) \
	.def("next", &iter_type::next)

BOOST_PYTHON_MODULE(abtree_c)
{
	using namespace boost::python;
	class_<py_item_iterator>("ItemIterator", no_init)
		.def("__iter__", &py_item_iterator::__iter__)
		.def("next", &py_item_iterator::next)
		;
	class_<py_store, boost::shared_ptr<py_store>, boost::noncopyable >("Store", init<std::string, bool, size_t, size_t, const object&>())
		.def("load", &py_store::load)
		.def("save", &py_store::save)
		.def("sync", &py_store::sync)
		;
	class_<py_tree, boost::shared_ptr<py_tree>, boost::noncopyable >("Tree", init<const boost::shared_ptr<py_store>&, const object&>())
		.def("getitem", &py_tree::getitem)
		.def("setitem", &py_tree::setitem)
		.def("delitem", &py_tree::delitem)
		.def("iter", &py_tree::iter)
		.def("len", &py_tree::len)
		.def("total", &py_tree::total)
		.def("key_at_index", &py_tree::key_at_index)
		.def("copy", &py_tree::copy)
		;
}
