
#include <boost/python.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
using namespace boost::python;

#define __for_python__

#include "abtree/abtree.h"
#include "abtree/disk_abtree.h"
#include "abtree/tree_walker.h"
#include "abtree/hilbert.h"

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

template<class TreeType>
class py_key_iterator
{
	typedef TreeType tree_t;
	typedef typename tree_t::const_iterator iterator_t;
public:
	py_key_iterator(const iterator_t& begin, const iterator_t& end) 
		: m_it(begin)
		, m_end(end)
	{}


	py_key_iterator __iter__()
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
		object r = m_it->first;
		m_it++;
		return r; 
	}
private:
	iterator_t m_it;
	iterator_t m_end;
};

class py_boolean
{
public:
	py_boolean(const object& lambda) 
		: m_lambda(lambda)
	{}

	bool operator()(const obj_count& o) const
	{
		return extract<bool>(m_lambda(o.obj));
	}

private:
	object m_lambda;	
};

template<class TreeType>
class py_tree;

class mem_less
{
public:
	mem_less(const object& obj)
		: m_object(obj)
	{}
	
	bool operator()(const object& a, const object& b) const 
	{
		return extract<int>(m_object["cmp"](a, b)) < 0; 
	}

private:
	object m_object;
};

class mem_agg
{
public:
	mem_agg(const object& obj)
		: m_object(obj)
	{}

	void operator()(obj_count& out, const obj_count& in) const 
	{
		out.count += in.count;
		out.obj = m_object["aggregate"](out.obj, in.obj); 
	}
	
private:
	object m_object;
};

typedef abtree_store<pytree_policy>::tree_type disk_tree;
typedef abtree<object, obj_count, mem_agg, mem_less> memory_tree;

typedef py_tree<disk_tree> py_disk_tree;
typedef py_tree<memory_tree> py_memory_tree;

class py_store : public boost::enable_shared_from_this<py_store>
{
	typedef disk_tree tree_t;
	typedef abtree_store<pytree_policy> store_t;
	typedef tree_t::const_iterator iterator_t;
public:
	py_store(const std::string& name, bool create, size_t max_unwritten, size_t max_lru, const object& policy)
		: m_store(name, create, max_unwritten, max_lru, pytree_policy(policy))
	{}

	store_t& get_store() { return m_store; }
	boost::shared_ptr<py_disk_tree> attach(const std::string& name, const object& policy);

	void mark() { m_store.mark(); }
	void revert() { m_store.revert(); }
	void sync() { m_store.sync(); }

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

template<class TreeType>
class py_find_iterator
{
	typedef TreeType tree_t;
	typedef typename tree_t::const_iterator iterator_t;
public:
	py_find_iterator(const boost::shared_ptr<py_store>& store, const tree_t& tree, const object& lambda)
		: m_store(store)
		, m_tree(tree)
		, m_functor(lambda)
		, m_it(m_functor, m_tree.get_root(), m_tree.get_height(), true)
		, m_end(m_functor, m_tree.get_root(), m_tree.get_height(), false)
	{}
		
	py_find_iterator __iter__()
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
		object r = m_it->first;
		m_it++;
		return r; 
	}
private:
	typedef forward_subset_iterator<tree_t, py_boolean> it_t;
	boost::shared_ptr<py_store> m_store;
	tree_t m_tree;
	py_boolean m_functor;
	it_t m_it;
	it_t m_end;
};

template<class TreeType>
class py_tree 
{
	typedef TreeType tree_t;
	typedef boost::shared_ptr<TreeType> tree_ptr_t;
	typedef typename tree_t::const_iterator iterator_t;
public:
	/* Only instantiated for disk_tree */
	py_tree(const boost::shared_ptr<py_store>& store, const std::string& name, const object& policy) 
		: m_store(store)
		, m_tree(m_store->get_store().attach(name, pytree_policy(policy)))
		, m_name(name)
	{}

	/* Only instantiated for disk_tree */
	py_tree(const boost::shared_ptr<py_store>& store, const object& policy)
		: m_store(store)
		, m_tree(m_store->get_store().create_tmp(pytree_policy(policy)))
        {}

	/* Only instantiated for mem_tree */
	py_tree(const object& policy)
		: m_tree(tree_ptr_t(new tree_t(mem_agg(policy), mem_less(policy))))
	{}

	const tree_t& get_tree() { return *m_tree; }

	object getitem(const object& key) 
	{ 
		object r;
		iterator_t it = m_tree->find(key);
		if (it == m_tree->end())
		{
			PyErr_SetString(PyExc_KeyError, "no such key");
			throw boost::python::error_already_set();
		}
		return it->second.obj;
	}

	void setitem(const object& key, const object& value) 
	{ 
		(*m_tree)[key] = value;
	}

	void delitem(const object& key)
	{
		m_tree->erase(key);
	}

	py_key_iterator<TreeType> iter(const object& start, const object& end)
	{
		range_t range = make_range(start, end);
		return py_key_iterator<TreeType>(range.first, range.second);
	}

	object total(const object& start, const object& end, const object& base)
	{
		range_t range = make_range(start, end);
		return m_tree->total(range.first, range.second, base).obj;
	}
	
	size_t len(const object& start, const object& end)
	{
		obj_count base(0);
		range_t range = make_range(start, end);
		return m_tree->total(range.first, range.second, base).count;
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
		m_tree->accumulate_until(range.first, total, range.second, index_functor(index));  // Do main walk
		if (range.first == range.second)
			 throw_index_error(); // If I ran out of elements, fail
		return range.first->first; // Otherwise, return key
	}

	boost::shared_ptr<py_tree> copy()
	{
		return boost::shared_ptr<py_tree>(new py_tree(*this));		
	}

	py_find_iterator<TreeType> find(const object& lambda)
	{
		return py_find_iterator<TreeType>(m_store, *m_tree, lambda);
	}

	object aggregate_until(const object& start, const object& end, const object& lambda)
	{
		range_t range = make_range(start, end);
		if (range.first == range.second) 
			return object();
		if (lambda(range.first->second.obj))
			return make_tuple(range.first->first, range.first->second.obj);
		obj_count total = range.first->second;  // Get first value for accumulator
		range.first++;  // Skip over it
		m_tree->accumulate_until(range.first, total, range.second, py_boolean(lambda));  // Do main walk
		if (range.first == range.second)
			return object();
		m_tree->get_policy().aggregate(total, range.first->second);	
		return make_tuple(range.first->first, total.obj);
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
				return std::make_pair(m_tree->begin(), m_tree->end());
			else
				return std::make_pair(m_tree->begin(), m_tree->lower_bound(end));
		}
		else
		{
			if (end == object())
				return std::make_pair(m_tree->lower_bound(start), m_tree->end());
			else
				return std::make_pair(m_tree->lower_bound(start), m_tree->lower_bound(end));
		}
	}
	/* Used by copy */
	py_tree(const py_tree& rhs)
		: m_store(rhs.m_store)
		, m_tree(tree_ptr_t(new tree_t(*rhs.m_tree)))
	{}

	boost::shared_ptr<py_store> m_store;  // Empty in memory case
	tree_ptr_t m_tree;
	std::string m_name;
};

boost::shared_ptr<py_disk_tree> py_store::attach(const std::string& name, const object& policy)
{
	return boost::shared_ptr<py_disk_tree>(new py_disk_tree(shared_from_this(), name, policy));
}

class hilbert_wrap
{
public:
	static int hilbert_cmp(boost::python::list& a, boost::python::list& b)
	{
		if (len(a) != len(b))
		{
			PyErr_SetString(PyExc_ValueError, "Hilbert comparison of unequal dimensions");
			throw boost::python::error_already_set();
		}
		size_t size = len(a);
		double* va = new double[size];
		double* vb = new double[size];
		for(size_t i = 0; i < size; i++)
		{
			va[i] = extract<double>(a[i]);
			vb[i] = extract<double>(b[i]);
		}
		return hilbert_ieee_cmp(size, va, vb);
		delete va;
		delete vb;
	}
};

BOOST_PYTHON_MODULE(abtree_c)
{
        PyEval_InitThreads();
	using namespace boost::python;
	class_<py_key_iterator<disk_tree> >("DiskKeyIterator", no_init)
		.def("__iter__", &py_key_iterator<disk_tree>::__iter__)
		.def("next", &py_key_iterator<disk_tree>::next)
		;
	class_<py_find_iterator<disk_tree> >("DiskFindIterator", no_init)
		.def("__iter__", &py_find_iterator<disk_tree>::__iter__)
		.def("next", &py_find_iterator<disk_tree>::next)
		;
	class_<py_key_iterator<memory_tree> >("MemoryKeyIterator", no_init)
		.def("__iter__", &py_key_iterator<memory_tree>::__iter__)
		.def("next", &py_key_iterator<memory_tree>::next)
		;
	class_<py_find_iterator<memory_tree> >("MemoryFindIterator", no_init)
		.def("__iter__", &py_find_iterator<memory_tree>::__iter__)
		.def("next", &py_find_iterator<memory_tree>::next)
		;
	class_<py_store, boost::shared_ptr<py_store>, boost::noncopyable >("Store", 
		init<std::string, bool, size_t, size_t, const object&>())
		.def("attach", &py_store::attach)
		.def("mark", &py_store::mark)
		.def("revert", &py_store::revert)
		.def("sync", &py_store::sync)
		;
	class_<py_disk_tree, boost::shared_ptr<py_disk_tree>, boost::noncopyable >("DiskTree", 
		init<const boost::shared_ptr<py_store>&, const object&>())
		.def("getitem", &py_disk_tree::getitem)
		.def("setitem", &py_disk_tree::setitem)
		.def("delitem", &py_disk_tree::delitem)
		.def("iter", &py_disk_tree::iter)
		.def("len", &py_disk_tree::len)
		.def("total", &py_disk_tree::total)
		.def("key_at_index", &py_disk_tree::key_at_index)
		.def("find", &py_disk_tree::find)
		.def("copy", &py_disk_tree::copy)
		;
	class_<py_memory_tree, boost::shared_ptr<py_memory_tree>, boost::noncopyable >("MemoryTree", 
		init<const object&>())
		.def("getitem", &py_memory_tree::getitem)
		.def("setitem", &py_memory_tree::setitem)
		.def("delitem", &py_memory_tree::delitem)
		.def("iter", &py_memory_tree::iter)
		.def("len", &py_memory_tree::len)
		.def("total", &py_memory_tree::total)
		.def("key_at_index", &py_memory_tree::key_at_index)
		.def("find", &py_memory_tree::find)
		.def("aggregate_until", &py_memory_tree::aggregate_until)
		.def("copy", &py_memory_tree::copy)
		;
	class_<hilbert_wrap>("Hilbert", no_init)
		.def("cmp", &hilbert_wrap::hilbert_cmp)
		.staticmethod("cmp")
		;
}
