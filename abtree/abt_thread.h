
/*
    Aggregate btree_base implementation
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

/* 
   Because boost seems to be too much work for all my friends to actually install
   (especially python folks), I'm removing all the 'non-header' parts of boost
*/

#ifndef __abt_thread_h__
#define __abt_thread_h__

class abt_lock;
class abt_condition;

class abt_mutex
{
	friend class abt_lock;
	friend class abt_condition;
public:
	abt_mutex()
	{
		pthread_mutex_init(&m_mutex, NULL);
	}
	~abt_mutex()
	{
		pthread_mutex_destroy(&m_mutex);
	}

	void lock()
	{
		pthread_mutex_lock(&m_mutex);
	}

	void unlock()
	{
		pthread_mutex_unlock(&m_mutex);
	}

private:
	bool m_held;
	pthread_mutex_t m_mutex;
};

class abt_lock
{
	friend class abt_condition;
public:
	abt_lock(abt_mutex& mutex) 
		: m_mutex(mutex)
	{
		pthread_mutex_lock(&m_mutex.m_mutex);
	}

	~abt_lock()
	{
		pthread_mutex_unlock(&m_mutex.m_mutex);
	}
	
private:
	abt_mutex& m_mutex;
};

class abt_condition
{
public:
	abt_condition()
	{
		pthread_cond_init(&m_cond, NULL);
	}

	~abt_condition()
	{
		pthread_cond_destroy(&m_cond);
	}

	void wait(abt_lock& lock)
	{
		pthread_cond_wait(&m_cond, &lock.m_mutex.m_mutex);
	}

	void notify_all()
	{
		pthread_cond_broadcast(&m_cond);	
	}
private:
	pthread_cond_t m_cond;
};

#endif
