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

#include "abtree/file_io.h"

file_reader::file_reader(FILE* file) 
	: m_user_file(true)
	, m_file(file) 
{}

file_reader::file_reader(const std::string& filename) 
	: m_user_file(false)
{
	m_file = fopen(filename.c_str(), "rb");
	if (!m_file) 
		throw io_exception("Unable to open file for reading: " + filename);
}

file_reader::~file_reader() 
{ 
	close(); 
}

void file_reader::close() 
{ 
	if (m_user_file || m_file == NULL) return;
	if (fclose(m_file) != 0) 
		throw io_exception("Unable to close file"); 
	m_file = NULL; 
}

int file_reader::base_read(char* buf, int len) 
{ 
	return fread(buf, 1, len, m_file); 
}
	
file_writer::file_writer(FILE* file) 
	: m_user_file(true)
	, m_file(file) 
{}

file_writer::file_writer(const std::string& filename) 
	: m_user_file(false)
{
	m_file = fopen(filename.c_str(), "wb");
	if (!m_file) 
		throw io_exception("Unable to open file for writing");
}

file_writer::~file_writer() 
{ 
	close(); 
}

int file_writer::base_write(const char* buf, int len) 
{
	return fwrite(buf, 1, len, m_file); 
}

int file_writer::base_flush() 
{ 
	if (m_file == NULL) return 0;
	return fflush(m_file);
}

int file_writer::base_close() 
{
	int r = 0;
	if (!m_user_file && m_file != NULL)
	{
		r = fclose(m_file);
		m_file = NULL;
	}
	return r;
}

file_io::file_io(const std::string& filename) 
	: m_file(fopen(filename.c_str(), "a+"))
{
	if (!m_file) 
		throw io_exception("Unable to open file for writing");
}

file_io::~file_io() 
{ 
	base_close(); 
}

off_t file_io::get_offset()
{
	off_t r = ftello(m_file);
	if (r == -1)
		throw io_exception("Error during get_offset");
	return r;
}

void file_io::seek(off_t offset)
{
	if (fseeko(m_file, offset, SEEK_SET) != 0)
		throw io_exception("Failed to seek");
}

void file_io::seek_end()
{
	if (fseeko(m_file, 0, SEEK_END) != 0)
		throw io_exception("Failed to seek");
}

int file_io::base_read(char* buf, int len) 
{ 
	return fread(buf, 1, len, m_file); 
}
	
int file_io::base_write(const char* buf, int len) 
{
	return fwrite(buf, 1, len, m_file); 
}

int file_io::base_flush() 
{ 
	if (m_file == NULL) return 0;
	return fflush(m_file);
}

int file_io::base_close() 
{
	int r = 0;
	r = fclose(m_file);
	return r;
}

