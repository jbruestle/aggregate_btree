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

#include <stdarg.h>
#include <memory.h>
#include <errno.h>
#include "abtree/io.h"

const static size_t k_bufsize = 16 * 1024;

std::string printstring(const char* format, ...)
{
        va_list vl;
        va_start(vl, format);

        char* buf;
        int len = vasprintf(&buf, format, vl);
        std::string r(buf, len);
        free(buf);
        va_end(vl);

        return r;
}

void writable::print(const char* format, ...)
{
	va_list vl;
	va_start(vl, format);

	char* buf;
	int len = vasprintf(&buf, format, vl);
	write(buf, len); 
	free(buf);
	va_end(vl);
}

bool readable::readline(std::string& line, size_t maxsize)
{
	char c;
	while(line.size() < maxsize)
	{
		size_t r = this->read(&c, 1);
		if (r == 0)
			return false;
		if (c == '\r')
			continue;
		if (c == '\n')
			return true;
		line.push_back(c);
	}
	throw io_exception("Line overflow in readable::readline");
}

read_wrapper::read_wrapper()
	: m_buf(new char[k_bufsize])
	, m_start(0)
	, m_end(0)
{}

read_wrapper::~read_wrapper()
{
	delete[] m_buf;
}

size_t read_wrapper::read(char* buf, size_t len)
{
	size_t tot_read = 0;

	/* First use pushback buffer if any */
	size_t read_from_buf = m_end - m_start;
	if (read_from_buf)
	{
		if (read_from_buf > len)
			read_from_buf = len;
		memcpy(buf, m_buf + m_start, read_from_buf);
		len -= read_from_buf;
		buf += read_from_buf;
		tot_read += read_from_buf;
		m_start += read_from_buf;
	}

	while(len)
	{
		int r = base_read(buf, len);
		if (r < 0)
			throw io_exception(printstring("IO error on read: %d, %d", r, errno));
		if (r == 0)
			return tot_read;
		tot_read += r;
		buf += r;
		len -= r;
	}

	return tot_read;
}

bool read_wrapper::readline(std::string& line, size_t maxlen)
{
	line.clear();
	while(line.size() < maxlen)
	{
		if (m_start == m_end)
		{
			int r = base_read(m_buf, k_bufsize);
			if (r < 0)
				throw io_exception("IO error on readline");
			if (r == 0)
				return false;
			m_start = 0;
			m_end = r;
		}
		char next = m_buf[m_start++];
		if (next == '\n')
			return true;
		else if (next != '\r')
			line.push_back(next);
	}
	throw io_exception("Long line in readline");
}

void write_wrapper::write(const char* buf, size_t len)
{
	while(len > 0)
	{
		int r = base_write(buf, len);
		if (r <= 0)
			throw io_exception("IO error during write");
		len -= r;
		buf += r;
	}
}

void write_wrapper::close()
{
	if (base_close() < 0)
		throw io_exception("Error during close");
}

void write_wrapper::flush()
{
	if (base_flush() < 0)
		throw io_exception("Error during flush");
}

