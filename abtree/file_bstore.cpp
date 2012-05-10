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

#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>
#include <arpa/inet.h>
//#include <bzlib.h>
#include "abtree/file_bstore.h"
#include "abtree/serial.h"

file_bstore::file_bstore(const std::string& dir, bool create)
	: m_size(0)
	, m_root(0)
	, m_next_slab(0)
	, m_cur_slab(NULL)
{
	lock_t lock(m_mutex);
	// Set the directory
	m_dir = dir;
	// Try to open the directory 
	DIR* d = opendir(dir.c_str());
	// If it's not there and create is set:
	if (d == NULL && create)
	{
		// Try to create the directory, thow if fail
		if (mkdir(dir.c_str(), 0755) != 0)
			throw io_exception("Unable to create directory");
		// Make an initial file
		next_file();
		return;
	}
	// Otherwise, let's see what's in the directory
	struct dirent* de = readdir(d);
	while(de)
	{
		std::string filename = de->d_name;
		int slab_num = 0;
		// If it matches our naming convention...
		if (filename.size() > 5 
			&& filename.substr(0, 5) == "data_")
		{
			// Todo: make sure this is really a number
			slab_num = atoi(filename.substr(5).c_str());
			add_file(filename);  // Add the file
			m_next_slab = std::max(m_next_slab, (unsigned int) (slab_num + 1));  // Prep the slab number
		}
		// Find next file
		de = readdir(d);
	}
	// If there was nothing, add a file
	if (m_slabs.size() == 0)
		next_file();
	else
		m_root = find_root();
}

file_bstore::~file_bstore()
{
	lock_t lock(m_mutex);

	// Close any open files (m_cur_slab is also in m_slabs)
	for(slabs_t::const_iterator it = m_slabs.begin(); it != m_slabs.end(); ++it)
		delete it->second.io;
}

off_t file_bstore::write_node(const std::vector<char>& record) 
{ 
	if (m_cur_slab == NULL)
		throw io_exception("file_store is not open");
	//unsigned int outlen = 101 * record.size() / 100 + 600;
	//std::vector<char> outbuf(4 + outlen);
	//*((uint32_t *) (outbuf.data())) = htonl(record.size());
	//BZ2_bzBuffToBuffCompress(&outbuf[4], &outlen, const_cast<char*>(&record[0]), record.size(), 1, 0, 0);
	//outbuf.resize(4 + outlen);
	lock_t lock(m_mutex); 
	//return write_record('N', outbuf); 
	return write_record('N', record); 
}

void file_bstore::write_root(const std::vector<char>& record) 
{ 
	if (m_cur_slab == NULL)
		throw io_exception("file_store is not open");
	lock_t lock(m_mutex); 
	m_root = write_record('R', record);
	//printf("Writing root record: %d\n", (int) m_root);
}

void file_bstore::read_node(off_t which, std::vector<char>& record) 
{ 
	if (m_cur_slab == NULL)
		throw io_exception("file_store is not open");
	//std::vector<char> inbuf;
	//{
	lock_t lock(m_mutex);
	//safe_read_record(which, 'N', inbuf);
	safe_read_record(which, 'N', record);
	//}
	//unsigned int destlen = ntohl(*((uint32_t *) (inbuf.data())));
	//record.resize(destlen);
	//BZ2_bzBuffToBuffDecompress(&record[0], &destlen, &inbuf[4], inbuf.size() - 4, 0, 0);
}

void file_bstore::read_root(std::vector<char>& record)
{
	if (m_cur_slab == NULL)
		throw io_exception("file_store is not open");
	lock_t lock(m_mutex);
	//printf("Reading root record: %d\n", (int) m_root);
	if (m_root == 0)
		record.clear();
	else
		safe_read_record(m_root, 'R', record);
}

void file_bstore::clear_before(off_t offset)
{
	if (m_cur_slab == NULL)
		throw io_exception("file_store is not open");
	lock_t lock(m_mutex);
	slabs_t::iterator itEnd = m_slabs.upper_bound(offset);
	if (itEnd == m_slabs.begin())
		return;  // Already done
	itEnd--;
	for(slabs_t::iterator it = m_slabs.begin(); it != itEnd; ++it)
	{
		slabs_t::iterator itNext = it;
		++itNext;
		//printf("Offset = %d, Deleting %d-%d\n", (int) offset, (int) it->first, (int) itNext->first);
		it->second.io->close();
		unlink(it->second.name.c_str());
	}
	m_slabs.erase(m_slabs.begin(), itEnd);
}
		
off_t file_bstore::find_root(off_t offset, file_io* f)
{
	off_t best_root = 0;
	char prefix = 0;
	f->seek(0);
	std::vector<char> record;
	off_t cur_offset = offset + f->get_offset();
	while(read_record(f, prefix, record))
	{
		if (prefix == 'R')
		{
			best_root = cur_offset;
		}
		cur_offset = offset + f->get_offset();
	}
	return best_root;
}

off_t file_bstore::find_root()
{
	off_t best_root = 0;
	slabs_t::reverse_iterator it = m_slabs.rbegin();
	while(it != m_slabs.rend())
	{
		best_root = find_root(it->first, it->second.io);
		if (best_root != 0)
			break;
		it++;
	}
	return best_root;
}

off_t file_bstore::write_record(char prefix, const std::vector<char>& record)
{
	off_t r = m_size;
	size_t slab_start = m_slabs.rbegin()->first;
	m_cur_slab->seek_end();
	m_cur_slab->write(&prefix, 1);
	serialize(*m_cur_slab, record.size());
	m_cur_slab->write(record.data(), record.size());
	m_size = slab_start + m_cur_slab->get_offset();
	if (m_size - slab_start >= (off_t) k_goal_slab_size)
		next_file();
	return r;
}

bool file_bstore::read_record(file_io* file, char& prefix, std::vector<char>& record)
{
	if (!file->read(&prefix, 1))
		return false;
	size_t s;
	deserialize(*file, s);
	record.resize(s);
	if (file->read(record.data(), s) != s)
		throw io_exception("EOF in read of data");
	return true;
}

bool file_bstore::read_record(off_t offset, char& prefix, std::vector<char>& record)
{
	if (offset == m_size)
		return false;

	slabs_t::iterator it = m_slabs.upper_bound(offset);
	if (it == m_slabs.begin())
		throw io_exception(printstring("Invalid offset on read: %d", (int) offset));
	it--;
	file_io* file = it->second.io;
	file->seek(offset - it->first);
	bool r = read_record(file, prefix, record);
	if (r == false && file != m_slabs.rbegin()->second.io)
		throw io_exception("End of file in inter-slab region");
	
	return true;
}

void file_bstore::safe_read_record(off_t offset, char req_prefix, std::vector<char>& record) 
{ 
	char prefix; 
	bool r = read_record(offset, prefix, record); 
	if (!r) throw io_exception("EOF in read of data"); 
	if (prefix != req_prefix) throw io_exception("Mismatched prefix"); 
}

void file_bstore::next_file()
{
	file_info fi;
	fi.name = m_dir + "/" + printstring("data_%d", m_next_slab++);
	m_cur_slab = new file_io(fi.name);
	fi.io = m_cur_slab;
	m_slabs.insert(std::make_pair(m_size, fi));
	std::vector<char> buf;
	vector_writer vw(buf);
	serialize(vw, m_size);
	write_record('S', buf);
}

void file_bstore::add_file(const std::string& name)
{
	file_info fi;
	fi.name = m_dir + "/" + name;
	file_io* file = new file_io(fi.name);
	fi.io = file;
	char prefix;
	std::vector<char> record;
	file->seek(0);
	if (!read_record(file, prefix, record))
		throw io_exception("File has no header data");
	if (prefix != 'S')
		throw io_exception("File has incorrect header data");

	vector_reader vr(record);
	off_t start_loc;
	deserialize(vr, start_loc);
	m_slabs.insert(std::make_pair(start_loc, fi));
	file->seek_end();
	off_t file_size = file->get_offset();
	if (start_loc + file_size > m_size)
	{
		m_cur_slab = file;
		m_size = start_loc + file_size;
	}
}

	
