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

template<class Key, class Value, class Context>
class bnode;

template<class Key, class Value, class Context>
class bnode_proxy;

template<class Key, class Value, class Context>
class pinned_proxy;

template<class Key, class Value, class Context>
class bnode_ptr;

template<class Key, class Value, class Context>
class bcache;

template<class Key, class Value, class Context>
class bstore;

template<class Key, class Value, class Context>
class bsnapshot;

template<class Key, class Value, class Context>
class biter;

template<class Key, class Value, class Context>
class btree;

#endif

