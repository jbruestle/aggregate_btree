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

#ifndef __vector_io_h__
#define __vector_io_h__

#include <vector>
#include "abtree/io.h"

class vector_writer : public writable
{
public:
	vector_writer(std::vector<char>& buffer) 
		: m_buffer(buffer)
	{}

	void write(const char* buf, size_t len)
	{
		size_t osize = m_buffer.size();
		m_buffer.resize(osize + len);
		memcpy(&m_buffer[osize], buf, len);
	}
private:
	std::vector<char>& m_buffer;
};

class vector_reader : public readable
{
public:
	vector_reader(std::vector<char>& buffer)
		: m_offset(0)
		, m_buffer(buffer)
	{}

	size_t read(char* buf, size_t len)
	{
		if (len > (m_buffer.size() - m_offset))
			len = m_buffer.size() - m_offset;
		memcpy(buf, &m_buffer[m_offset], len);
		m_offset += len;
		return len;
	}
private:
	size_t m_offset;
	std::vector<char>& m_buffer;
};

#endif

