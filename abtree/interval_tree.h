
#include "abtree.h"
#include <stdio.h>

template<class value_t, class position_t>
class default_interval_functor
{
public:
	std::pair<position_t, position_t> operator()(const value_t& v) const
	{
		return std::make_pair(v.get_start(), v.get_end());
	}
};

template<
	class Value, 
	class Position, 
	class IntervalGet = default_interval_functor<Value, Position>,
	class ValueCmp = std::less<Value>
>
class interval_tree 
{
	typedef Value value_type;
	typedef Value key_type;
	typedef Position position_type;
	typedef IntervalGet interval_functor_type;
	typedef ValueCmp base_value_compare;
	
private:
	position_type get_start(const value_type& v) const { return m_interval_functor(v).first; }
	position_type get_end(const value_type& v) const { return m_interval_functor(v).first; }

	struct augmented_key;

	struct augmented_key_cmp
	{
		augmented_key_cmp(const base_value_compare& val_cmp)
			: m_val_cmp(val_cmp)
		{}
		bool operator()(const augmented_key& a, const augmented_key& b) const
		{
			if (a.start < b.start)
				return true;
			if (b.start < a.start)
				return false;
			if (a.value == NULL && b.value != NULL)
				return true;
			if (a.value == NULL || b.value == NULL)
				return false;
			return m_val_cmp(*a.value, *b.value);
		}
	private:
		base_value_compare m_val_cmp;
	};

	struct position_aggregate
	{
		void operator()(position_type& out, const position_type& in) const
		{
			out = std::max(out, in);
		}
	};

	struct augmented_key
	{
		//augmented_key()
		//	: start()
		//	, value(NULL)
		//{}

		augmented_key(position_type _start)
			: start(_start)
			, value(NULL)
		{}
		augmented_key(const position_type& _start, const value_type* rhs)
			: start(_start)
			, value(rhs)
		{}

		position_type start;
		const value_type* value;
	};

	typedef abtree<augmented_key, position_type, position_aggregate, augmented_key_cmp> map_t;
public:
	class const_iterator : public boost::iterator_facade<
		const_iterator,
		const value_type,
		boost::bidirectional_traversal_tag
	>
	{
		friend class boost::iterator_core_access;
		friend class interval_tree;
		typedef typename map_t::const_iterator inner_iterator;
	public:
		const_iterator() {}

	private:
		const_iterator(const inner_iterator& it) : m_it(it) {}
		void increment() { m_it++; }
		void decrement() { m_it--; }
		bool equal(const_iterator const& other) const { return m_it == other.m_it; }
                const value_type& dereference() const { return *m_it->first.value; }

		inner_iterator m_it;
	};	

	struct  has_overlap_functor
	{
		has_overlap_functor(const position_type& start) : m_start(start) {}
		bool operator()(const position_type& end) const { return m_start < end; }
		position_type m_start;
	};

	class overlap_iterator : public boost::iterator_facade<
		overlap_iterator,
		const value_type,
		boost::forward_traversal_tag
	>
	{
		friend class boost::iterator_core_access;
		friend class interval_tree;
	public:
		overlap_iterator() 
			: map(NULL)
			, search_start()
			, search_end()
		{}

	private:
		overlap_iterator(const map_t& _map, const position_type& _search_start, const position_type& _search_end, bool is_end)
			: map(&_map)
			, search_start(_search_start)
			, search_end(_search_end)
		{
			if (is_end)
			{
				cur = map->end(); 
				return;
			}

			// Get the smallest element
			cur = map->begin();
			skip_ahead();
		}
	
		void skip_ahead()
		{
			// Go forward until we hit the first 'overlapping' element
			position_type pos = search_start;
			map->accumulate_until(cur, pos, map->end(), has_overlap_functor(search_start));
			// If we are no longer in overlap range, go to end
			if (cur != map->end() && !(cur->first.start < search_end)) 
				cur = map->end();
		}

		void increment()
		{
			cur++;
			skip_ahead();
		}

		bool equal(const overlap_iterator& other) const
		{
			if (search_start != other.search_start) return false;
			if (search_end != other.search_end) return false;
			if (cur != other.cur) return false;
			return true;
		}

		const value_type& dereference() const
		{
			return *cur->first.value;
		}

		const map_t* map;
		position_type search_start;
		position_type search_end;
		typename map_t::const_iterator cur;
	};

	interval_tree(const base_value_compare& vc = base_value_compare())
		: m_map(position_aggregate(), augmented_key_cmp(vc))
	{}

	typedef std::pair<overlap_iterator, overlap_iterator> range_t;
	void insert(const value_type& val)
	{
		assert(!(get_end(val) < get_start(val)));
		value_type* copy = new value_type(val);
		m_map.insert(std::make_pair(
			augmented_key(get_start(*copy), copy), get_end(*copy)));
	}

	void erase(const value_type& val)
	{
		erase(find(val));
	}
	
	void erase(const const_iterator& it)
	{
		if (it.m_it == m_map.end())
			return;
		const value_type* ptr= it.m_it->first.value;
		m_map.erase(it.m_it);
		delete ptr;
	}

	void erase(const overlap_iterator& oit)
	{
		if (oit.cur == m_map.end())
			return;
		value_type* ptr= oit.cur->first.value;
		m_map.erase(oit.cur);
		delete ptr;
	}

	const_iterator begin() const { return const_iterator(m_map.begin()); }
	const_iterator end() const { return const_iterator(m_map.end()); }

	const_iterator find(const value_type& value) const
	{
		augmented_key ak(get_start(value), &value);
		return const_iterator(m_map.find(ak));
	}

	range_t find(const position_type& start, const position_type& end) const
	{
		return std::make_pair(
			overlap_iterator(m_map, start, end, false), 
			overlap_iterator(m_map, start, end, true)
		); 
	}

	size_t size() const { return m_map.size(); }

private:
	map_t m_map;
	interval_functor_type m_interval_functor;
};

 
