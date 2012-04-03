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

#ifndef __bdecl_h__
#define __bdecl_h__

#include <boost/shared_ptr.hpp>

template<class Policy>
class bnode;

template<class Policy>
class bnode_proxy;

template<class Policy>
class pinned_proxy;

template<class Policy>
class bnode_cache_ptr;

template<class Policy>
class bcache;

template<class Policy>
class bcache_nop;

template<class Policy>
class biter;

template<class Policy>
class btree;

template<class Policy, bool use_cache>
struct apply_policy_impl
{
	typedef bcache<Policy> cache_t;
	typedef bnode_cache_ptr<Policy> ptr_t;
	typedef cache_t* cache_ptr_t;
};

template<class Policy>
struct apply_policy_impl<Policy, false>
{
	typedef bcache_nop<Policy> cache_t;
	typedef boost::shared_ptr<bnode<Policy> > ptr_t;
	typedef boost::shared_ptr<cache_t> cache_ptr_t;
}; 

template<class Policy>
struct apply_policy
{
	typedef typename apply_policy_impl<Policy, Policy::use_cache>::cache_t cache_t;
	typedef typename apply_policy_impl<Policy, Policy::use_cache>::ptr_t ptr_t;
	typedef typename apply_policy_impl<Policy, Policy::use_cache>::cache_ptr_t cache_ptr_t;
};
 
#endif

