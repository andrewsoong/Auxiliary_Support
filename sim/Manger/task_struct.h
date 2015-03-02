#ifdef _WIN32
#pragma once
#endif

#ifndef TASK_STRUCT
#define  TASK_STRUCT

#include "pthread.h"
class task_struct
{
public:
	task_struct(void);
	virtual ~task_struct(void);
public:
	int group_index_from;
	int group_index_end;
	int group_index_currrent;
private:
	pthread_mutex_t m_lock;
public:
	int get_task(int& n);

};
#endif
