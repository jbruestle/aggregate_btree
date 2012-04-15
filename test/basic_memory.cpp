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

#include "abtree/abtree.h"
#include "gtest/gtest.h"

TEST(memory_btree, basic)
{
	typedef abtree<int, int> bt_t;
	typedef bt_t::iterator it_t;
	bt_t tree;
	it_t it, it_end = tree.end();
	for(size_t i = 0; i < 100; i++)
	{
		int k = random() % 1000;
		int v = random() % 100;
		tree[k] = v;
	}
	bt_t tree2 = tree;
	it = tree.begin();
	int computed_total = 0;
	for(; it != it_end; ++it)
	{
		computed_total += it->second;
	}
	ASSERT_NE(computed_total, 0);
	ASSERT_EQ(tree.total(tree.begin(), tree.end()), computed_total);
	ASSERT_EQ(tree2.total(tree2.begin(), tree2.end()), computed_total);
	tree.clear();
	ASSERT_EQ(tree.total(tree.begin(), tree.end()), 0);
	ASSERT_EQ(tree2.total(tree2.begin(), tree2.end()), computed_total);
}

