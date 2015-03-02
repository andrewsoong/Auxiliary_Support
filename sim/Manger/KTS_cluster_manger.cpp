/***************************************************************/ 
/*  Creat by BeiJing Kingtansin Technology Co. Ltd
;*
;*  Name:  KTS_cluster_manger 
;*  
;*  Description:
;*
;* 
;*  Arguments:    
;* 
;*  Name        Type        Description
;*     ---------------------------------------------------
;*              
;*
;*  Subroutines needed:
;*
;*
;*  Record of revisions:
;*        Date          Programmer         Description of change
;*    ============    ==============    ========================
;*     12/05/2014       Yuan Wentian       Original code
;*  
;* *************************************************************/
#include "work.h"

//#define  MPICH_IGNORE_CXX_SEEK

#include "mpi.h"
#include <iostream>
#include <vector>
#include "stdio.h"
using namespace std;
#include "task_struct.h"

task_struct m_task_struct;
int task_assign_granularity=1;
pthread_mutex_t  send_lock;
pthread_mutex_t  recv_lock;

void* control_work_node_m(void* worker_num)
{
	int flag=100;
	int n=task_assign_granularity;
	int complete_work_num=0;
	int work_num=*((int*)(worker_num));
	while(true)
	{
		int send_struct[2];
		int recv_struct;
		MPI_Status status;
		MPI_Recv(&recv_struct,1,MPI::INT,MPI_ANY_SOURCE,101,MPI_COMM_WORLD,&status);
		if (recv_struct==-1)
		{
			complete_work_num++;
			if (complete_work_num==work_num)
			{
				break;
			}
		}
		else
		{
			
			//compute new task assign
			int index_from=m_task_struct.get_task(n);
			if (index_from<0)  //no leave task
			{
				send_struct[0]=-1;  //task index from
				send_struct[1]=0;			//task num

				MPI::COMM_WORLD.Send(send_struct,2,MPI::INT,status.MPI_SOURCE,100);
			}
			else
			{
				send_struct[0]=index_from;  //task index from
				send_struct[1]=n;			//task num

				MPI::COMM_WORLD.Send(send_struct,2,MPI::INT,status.MPI_SOURCE,100);

			}
		}
		

	}
	return NULL;
}


int main(int argc, char* argv[])
{
	int my_id;
	int num_of_process(0);
	if (argc!=2)
	{
		printf("%s\n",argv[1]);
		printf("input argv error!");
		return -1;
	}
	MPI::Init(argc,argv);
	num_of_process=MPI::COMM_WORLD.Get_size();
	my_id=MPI::COMM_WORLD.Get_rank();
	int recv_struct[2];
	int send_struct(0);
	char worker_name[MPI_MAX_PROCESSOR_NAME];
	int namelen(0);
	MPI::Get_processor_name(worker_name,namelen);
	/************************************************************************/
	/* master node                                                                     */
	/************************************************************************/
	if (0==my_id)
	{

		int tasks_num = 100;
		//read config file to get task info
		m_task_struct.group_index_from=0;
		m_task_struct.group_index_end=tasks_num-1;
		m_task_struct.group_index_currrent=0;
		//end of init task info
		int task_num=m_task_struct.group_index_end-m_task_struct.group_index_from+1;
		printf("count of task:%d\n",task_num);
		printf("count of work node:%d\n",num_of_process);

		int* worker_id=new int[num_of_process-1];
	
		for (int i=1;i<num_of_process;i++)
		{
			worker_id[i-1]=i;
			int n=task_assign_granularity;
			int send_struct[2];
			//compute new task assign
			int index_from=m_task_struct.get_task(n);
			if (index_from<0)  //no leave task
			{
				send_struct[0]=-1;  //task index from
				send_struct[1]=0;			//task num
				MPI::COMM_WORLD.Send(send_struct,2,MPI::INT,worker_id[i-1],100);
			}
			else
			{
				send_struct[0]=index_from;  //task index from
				send_struct[1]=n;			//task num
				MPI::COMM_WORLD.Send(send_struct,2,MPI::INT,worker_id[i-1],100);
			
			}
		}
		pthread_t control_pid;
		int worker_num=num_of_process-1;
		if (worker_num>0)
		{
			pthread_create(&control_pid,NULL,control_work_node_m,&worker_num);
		}
		/************************************************************************/
		/* master do some work                                                                     */
		int n=task_assign_granularity;
		while(true)
		{
			//compute new task assign
			int index_from=m_task_struct.get_task(n);
			if (index_from<0)  //no leave task
			{
				printf("worker=%d:task_complete!\n",my_id);
				break;
			}
			else
			{
				printf("worker=%s,%d:task_running:\n",worker_name,my_id);
				for (int i=0;i<n;i++)
				{
					printf("task:%d;\t",index_from+i);

				}
				srand((int)time(0));
				int recv_struct_m[2];
				recv_struct_m[0]=index_from;
				recv_struct_m[1]=n;
				string pro=argv[1];
				process_task(recv_struct_m,pro);
			}
		}
		/************************************************************************/
		if (worker_num>0)
		{
			pthread_join(control_pid,NULL);
		}
		delete []worker_id;
	}
	else  //work node
	{
		while (true)
		{
			MPI::COMM_WORLD.Recv(recv_struct,2,MPI::INT,0,100);
			//do something
			if (recv_struct[0]<0)  //ÎÞÊ£ÓàÈÎÎñ
			{
				printf("worker=%d:task_complete!\n",my_id);
				send_struct=-1;
				MPI_Send(&send_struct,1,MPI::INT,0,101,MPI_COMM_WORLD);
				break;
			}
			else
			{
				printf("worker=%s,%d:task_running:\n",worker_name,my_id);
				for (int i=0;i<recv_struct[1];i++)
				{
					printf("task:%d;\t",recv_struct[0]+i);

				}
				string pro=argv[1];
				process_task(recv_struct,pro);
				send_struct=0;
				MPI::COMM_WORLD.Send(&send_struct,1,MPI::INT,0,101);
			}

		}	
	}
	MPI::Finalize();
	return 0;
}




