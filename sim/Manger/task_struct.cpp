#include "task_struct.h"

task_struct::task_struct(void):
group_index_from(0),
group_index_end(0),
group_index_currrent(0)
{
	pthread_mutex_init(&m_lock,NULL);
}

task_struct::~task_struct(void)
{
	pthread_mutex_destroy(&m_lock);
}

int task_struct::get_task(int& n)
{
	int old_index(0);
	pthread_mutex_lock(&m_lock);
	old_index=group_index_currrent;
	if(group_index_currrent>group_index_end)
	{
		old_index=-1;
		n=0;
	}
	else
	{
		group_index_currrent+=n;
		n=group_index_currrent>group_index_end?(group_index_end-old_index+1):n;
	}
	pthread_mutex_unlock(&m_lock);
	return old_index;
}
