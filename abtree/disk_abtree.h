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

#ifndef __disk_abtree_h__
#define __disk_abtree_h__

#include "abtree/btree.h"

template<class BasePolicy, class File = file_bstore>
class disk_abtree : public btree_impl::btree_base<btree_impl::disk_policy<BasePolicy, File> >
{
	typedef btree_impl::disk_policy<BasePolicy, File> policy_t;
	typedef btree_impl::bcache<policy_t> cache_t;
	typedef btree_impl::btree_base<policy_t> base_t;
	typedef typename btree_impl::apply_policy<policy_t>::cache_ptr_t cache_ptr_t;

public:
	class store_type : public File
	{
		friend class disk_abtree;
	public:
		store_type(size_t max_unwritten, size_t max_lru)
			: File() 
			, m_cache(*this, max_unwritten, max_lru)
		{}
		void clean_one() { m_cache.clean_one(); }
	private:
		cache_t m_cache;
	};			

	disk_abtree(store_type& store, const BasePolicy& policy = BasePolicy()) 
		: base_t(&store.m_cache, policy_t(policy))
	{}
	disk_abtree(store_type& store, const std::string& name, const BasePolicy& policy = BasePolicy()) 
		: base_t(&store.m_cache, name, policy_t(policy)) 
	{}
	disk_abtree(const disk_abtree& rhs) 
		: base_t(rhs) 
	{}
};

#endif
