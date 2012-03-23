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

#ifndef __io_h__
#define __io_h__

#include <string>
#include <stdio.h>

// Makes printing to strings easy
std::string printstring(const char* format, ...);

// Exception class for io abstraction
class io_exception
{
public:
	io_exception(const std::string& message) : m_message(message) {}
	const std::string& message() const { return m_message; }
private:
	std::string m_message;
};

class readable
{
public:
	virtual ~readable() {}

	// Always reads until len or EOF, returns total read, throws on err 
	virtual size_t read(char* buf, size_t len) = 0;

	// Reads till an EOF or EOL, return true if line was full (includes EOL), false if EOF first
	// In the case of return of false, line contains chars between last EOL and EOF
	// Never reads more than maxlen, throws if err or long line 
	// Readable provides a (terrible) default implementation, read_wrapper's is better
	virtual bool readline(std::string& line, size_t maxlen);
};

/* A helper class to wrap objects with unix read semantics (-1 on err, not full reads, etc) */
class read_wrapper : public readable
{
public:
	read_wrapper();
	~read_wrapper();
	size_t read(char* buf, size_t len);
	bool readline(std::string& line, size_t maxlen);

private:
	virtual int base_read(char* buf, int len) = 0;
	char* m_buf;
	size_t m_start;
	size_t m_end;
};

class writable
{
public:
	virtual ~writable() {}

	// A fun helper function
	void print(const char* format, ...);

	// Always does a full write, throws on errors, may do buffering prior to close
	virtual void write(const char* buf, size_t len) = 0;

	// Flush data before returning
	virtual void flush() {}

	// Since you can write, it must make sense to 'finish' somehow, throws on error
	virtual void close() {} 
};

/* A helper class to wrap objects with unix write semantics */
class write_wrapper : public writable
{
public:
	void write(const char* buf, size_t len);
	void flush();
	void close();

private:
	virtual int base_close() { return 0; }
	virtual int base_flush() { return 0; }
	virtual int base_write(const char *buf, int len) = 0;
};

#endif

