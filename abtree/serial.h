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

#ifndef __serial_h__
#define __serial_h__

#include <string>
#include <vector>
#include "abtree/io.h"

void serialize(writable& dest, int x);
void deserialize(readable& src, int& x);
void serialize(writable& dest, off_t x);
void deserialize(readable& src, off_t& x);
void serialize(writable& dest, size_t x);
void deserialize(readable& src, size_t& x);

void serialize(writable& dest, const std::vector<char>& v);
void deserialize(readable& src, std::vector<char>& v);
void serialize(writable& dest, const std::string& str);
void deserialize(readable& src, std::string& str);

#endif 

