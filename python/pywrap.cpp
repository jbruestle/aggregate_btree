
#include <boost/python.hpp>
#include <boost/shared_ptr.hpp>
using namespace boost::python;

#include <abtree.h>

class pytree_policy
{
public:
        static const bool use_cache = true;
        static const size_t min_size = 10;
        static const size_t max_size = 20;
        typedef object key_t;
        typedef object value_t;
        typedef file_bstore store_t;

	pytree_policy()
	{}

	pytree_policy(object policy) 
		: m_policy(policy)
	{}

	bool less(const object& a, const object& b) const 
	{
		return extract<int>(m_policy["cmp"](a, b)) < 0; 
	}

	void aggregate(object& out, const object& in) const 
	{
		out = m_policy["aggregate"](out, in); 
	}

	void serialize(writable& out, const object& k, const object& v) const 
	{
		::serialize(out, extract<std::string>(m_policy["serialize"](k)));
		::serialize(out, extract<std::string>(m_policy["serialize"](v)));
	}
	void deserialize(readable& in, object& k, object& v) const 
	{ 
		std::string tmp;
		::deserialize(in, tmp); 
		k = m_policy["deserialize"](tmp);
		::deserialize(in, tmp); 
		v = m_policy["deserialize"](tmp);
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

typedef btree_impl::btree_base<pytree_policy> tree_t;
typedef btree_impl::bcache<pytree_policy> cache_t;
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
		object r = make_tuple(m_it->first, m_it->second);
		m_it++;
		return r; 
	}

private:
	iterator_t m_it;
	iterator_t m_end;
};

class py_store
{
public:
	py_store(const std::string& name, bool create)
	{
		printf("Opening store\n");
		m_store.open(name, create);
	}

	~py_store()
	{
		printf("Closing store\n");
		m_store.close();
	}
	file_bstore& get_store() { return m_store; }

private:
	file_bstore m_store;
};

class py_tree 
{
public:
	py_tree(boost::shared_ptr<py_store> store, const object& policy)
		: m_cache(new cache_t(
			store->get_store(),
			extract<size_t>(policy["max_unwritten"]),
			extract<size_t>(policy["max_lru"]),
			pytree_policy(policy)))
		, m_name(extract<std::string>(policy["name"]))
		, m_tree(&*m_cache, m_name)
	{
		printf("Constructed tree!\n");
	}

	object __getitem__(object key) 
	{ 
		object r;
		iterator_t it = m_tree.find(key);
		if (it == m_tree.end())
		{
			PyErr_SetString(PyExc_KeyError, "no such key");
			throw boost::python::error_already_set();
		}
		return it->second;
	}

	void __setitem__(object key, object value) 
	{ 
		m_tree[key] = value;
	}

	void __delitem__(object key)
	{
		m_tree.erase(key);
	}

	py_item_iterator __iter__()
	{
		return py_item_iterator(m_tree.begin(), m_tree.end());
	}

	size_t __len__()
	{
		return m_tree.size();
	}

	void sync()
	{
		m_tree.sync(m_name);
	}

	boost::shared_ptr<py_tree> copy()
	{
		return boost::shared_ptr<py_tree>(new py_tree(*this));		
	}

private:
	py_tree(const py_tree& rhs)
		: m_cache(rhs.m_cache)
		, m_name(rhs.m_name)
		, m_tree(rhs.m_tree)
	{}

	boost::shared_ptr<cache_t> m_cache;
	std::string m_name;
	tree_t m_tree;
};

#define DEF_ITER(pyname, iter_type) class_<iter_type>(pyname, no_init) \
	.def("__iter__", &iter_type::__iter__) \
	.def("next", &iter_type::next)

BOOST_PYTHON_MODULE(btree_c)
{
	using namespace boost::python;
	class_<py_item_iterator>("ItemIterator", no_init)
		.def("__iter__", &py_item_iterator::__iter__)
		.def("next", &py_item_iterator::next);
	class_<py_store, boost::shared_ptr<py_store>, boost::noncopyable >("Store", init<std::string, bool>());
	class_<py_tree, boost::shared_ptr<py_tree>, boost::noncopyable >("Tree", init<boost::shared_ptr<py_store>, object>())
		.def("__getitem__", &py_tree::__getitem__)
		.def("__setitem__", &py_tree::__setitem__)
		.def("__delitem__", &py_tree::__delitem__)
		.def("__iter__", &py_tree::__iter__)
		.def("__len__", &py_tree::__len__)
		.def("sync", &py_tree::sync)
		.def("copy", &py_tree::copy);
}
