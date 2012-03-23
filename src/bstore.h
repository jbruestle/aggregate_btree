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

#ifndef __bstore_h__
#define __bstore_h__

#include <boost/thread.hpp>
#include <vector>
#include <map>

#include "bdecl.h"
#include "vector_io.h"
#include "file_io.h"
#include "serial.h"

class bstore_base
{
	const static off_t k_goal_slab_size = 100*1024;  // 100 Kb slabs

	struct file_info
	{
		std::string name;
		file_io* io;
	};
	
	typedef std::map<off_t, file_info> slabs_t;
	typedef boost::recursive_mutex mutex_t;
	typedef boost::unique_lock<mutex_t> lock_t;

public:
	bstore_base();

	void close();  // Closes a bstroe
	void open(const std::string& dir, bool create = false);  // Opens, maybe creates
	off_t get_root() { lock_t lock(m_mutex); return m_root; }
	void clear_before(off_t lowest);
protected:
	void next_file(); // Create a new output file
	void add_file(const std::string& name); // Add a new i/o file

	// Finds roots during reload
	off_t find_root(off_t offset, file_io* f);
	off_t find_root();

	off_t write_record(char prefix, const std::vector<char>& record);  
	bool read_record(file_io* f, char& prefix, std::vector<char>& record);  
	bool read_record(off_t offset, char& prefix, std::vector<char>& record);  
	

	off_t m_size;  // The current 'logical size'
	off_t m_root;  // The current root offset
	unsigned int m_next_slab;  // The next slab number to use
	std::string m_dir;  // The directory the slabs live in
	file_io* m_cur_slab;  // The current slab
	slabs_t m_slabs;  // All the slabs
	mutex_t m_mutex;  // IO mutex, could be per slab, but I'm lazy
};

template<class Key, class Value, class Context>
class bstore : public bstore_base
{
	typedef bcache<Key, Value, Context> cache_t;
	typedef bnode<Key, Value, Context> node_t;
public:
	off_t write_node(const node_t& bnode)
	{
		std::vector<char> buf;
		vector_writer io(buf);
		bnode.serialize(io);
		off_t r = write_record('N', buf);
		return r;
	}

	void write_root(off_t off, off_t oldest)
	{
		std::vector<char> buf;
		vector_writer io(buf);
		serialize(io, off);
		serialize(io, oldest); 
		off_t r = write_record('R', buf);
		m_root = r;
	}
	

	void read_node(off_t loc, node_t& bnode, cache_t& cache)
	{
		std::vector<char> buf;
		char prefix = 0;
		read_record(loc, prefix, buf);
		if (prefix != 'N')
			throw io_exception("Invalid prefix in read_node");
		vector_reader io(buf);
		bnode.deserialize(io, cache);
	}

	void read_root(off_t loc, off_t& off, off_t& oldest)
	{
		std::vector<char> buf;
		char prefix = 0;
		read_record(loc, prefix, buf);
		if (prefix != 'R')
			throw io_exception("Invalid prefix in read_root");
		vector_reader io(buf);
		deserialize(io, off);
		deserialize(io, oldest);
	}
};

#endif
