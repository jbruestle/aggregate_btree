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


#ifndef __file_io_h__
#define __file_io_h__

#include "abtree/io.h"

class file_reader : public read_wrapper
{
public:
	file_reader(FILE* file);
	file_reader(const std::string& filename);
	~file_reader();
	void close();

private:
	int base_read(char* buf, int len);

	bool m_user_file;
	FILE* m_file;
};

class file_writer : public write_wrapper
{
public:
	file_writer(FILE* file);
	file_writer(const std::string& filename);
	~file_writer();

private:
	int base_write(const char* buf, int len);
	int base_flush();
	int base_close();

	bool m_user_file;
	FILE* m_file;
};

class file_io : public read_wrapper, public write_wrapper
{
public:
	file_io(const std::string& filename);
	~file_io();

	off_t get_offset();
	void seek(off_t offset);
	void seek_end();

private:
	int base_read(char* buf, int len);
	int base_write(const char* buf, int len);
	int base_flush();
	int base_close();

	FILE* m_file;
};

#endif

