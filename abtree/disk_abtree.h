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
class abtree_store
{
public:
	typedef btree_impl::btree_base<btree_impl::disk_policy<BasePolicy, File> > tree_type;
	typedef boost::shared_ptr<tree_type> tree_ptr_t;
private:
	typedef btree_impl::disk_policy<BasePolicy, File> policy_t;
	typedef btree_impl::bcache<policy_t> cache_t;
	typedef btree_impl::btree_base<policy_t> base_t;
	typedef typename btree_impl::apply_policy<policy_t>::cache_ptr_t cache_ptr_t;
public:
	abtree_store(const std::string& dir, bool create, size_t max_unwritten, size_t max_lru, const BasePolicy& policy = BasePolicy())
		: m_file(dir, create)
		, m_def_policy(policy_t(policy))
		, m_cache(m_file, max_unwritten, max_lru, policy_t(policy))
	{}

	tree_ptr_t attach(const std::string& name, const BasePolicy& policy = BasePolicy()) { 
		return m_cache.attach(name, policy_t(policy));
	}
	tree_ptr_t create_tmp(const BasePolicy& policy = BasePolicy())
	{
		return tree_ptr_t(new tree_type(&m_cache, policy));
	}

	void mark() { m_cache.mark(); }
	void revert() { m_cache.revert(); }
	void sync() { m_cache.sync(); }

private:
	File m_file;
	policy_t m_def_policy;
	cache_t m_cache;
};

#endif
