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
		// This is lame, I shouldn't be using derivation here
		store_type(const std::string& dir, bool create, size_t max_unwritten, size_t max_lru, const BasePolicy& policy = BasePolicy())
			: File(dir, create) 
			, m_def_policy(policy_t(policy))
			, m_cache(*this, max_unwritten, max_lru, policy_t(policy))
		{}
			
		void save(const std::string& name, const disk_abtree& tree)
		{
			m_cache.save(name, tree);
		}

		disk_abtree load(const std::string& name, const BasePolicy& policy = BasePolicy())
		{
			return disk_abtree(m_cache.load(name, policy_t(policy)));
		}
		
		void mark() { m_cache.mark(); }
		void revert() { m_cache.revert(); }
		void sync() { m_cache.sync(); }
	private:
		policy_t m_def_policy;
		cache_t m_cache;
	};	

	friend class store_type;	
	disk_abtree(store_type& store, const BasePolicy& policy = BasePolicy()) 
		: base_t(&store.m_cache, policy_t(policy))
	{}
	disk_abtree(const disk_abtree& rhs) 
		: base_t(rhs) 
	{}
private:
	disk_abtree(const base_t& base)
		: base_t(base) 
	{}

};

#endif
