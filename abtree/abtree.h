/*
    Aggregate btree_base implementation
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

#ifndef __abtree_h__
#define __abtree_h__

#include "abtree/btree.h"

template<class Key, class Value, class Aggregate = btree_impl::plus_equal<Value>, class Compare = std::less<Key> >
class abtree : public btree_impl::btree_base<btree_impl::memory_policy<Key, Value, Aggregate, Compare> >
{
	typedef btree_impl::memory_policy<Key, Value, Aggregate, Compare> policy_t;
	typedef typename btree_impl::apply_policy<policy_t>::cache_ptr_t cache_ptr_t;
	typedef btree_impl::btree_base<policy_t> base_t;
	typedef btree_impl::bcache_nop<policy_t> cache_t;
public:
	abtree(const Aggregate& aggregate = Aggregate(), const Compare& less = Compare())
		: base_t(cache_ptr_t(new cache_t()), policy_t(aggregate, less)) {}
	abtree(const abtree& rhs) : base_t(rhs) {}
};

#endif
