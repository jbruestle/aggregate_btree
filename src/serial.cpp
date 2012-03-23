
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

#include "serial.h"
#include <assert.h>

template<class IntType>
static void write_variable(writable& dest, IntType x)
{
	assert(x >= 0);
	while(true)
	{
		unsigned char c = (x & 0x7f);
		x >>= 7;
		if (x == 0)
		{
			dest.write((const char*) &c, 1);
			return;
		}
		c |= 0x80;
		dest.write((const char*) &c, 1);
	}
}

template<class IntType>
static void read_variable(readable& src, IntType& x)
{
	x = 0;
	int shift = 0;
	while(true)
	{
		unsigned char c;
		if (src.read((char *) &c, 1) != 1)
			throw io_exception("Invalid number end");
		x |= (c & 0x7f) << shift;
		if ((c & 0x80) == 0)
			return;
		shift += 7;
	}	
}

void serialize(writable& dest, int x) { write_variable(dest, x); }
void deserialize(readable& src, int& x) { read_variable(src, x); }
void serialize(writable& dest, off_t x) { write_variable(dest, x); }
void deserialize(readable& src, off_t& x) { read_variable(src, x); }
void serialize(writable& dest, size_t x) { write_variable(dest, x); }
void deserialize(readable& src, size_t& x) { read_variable(src, x); }

