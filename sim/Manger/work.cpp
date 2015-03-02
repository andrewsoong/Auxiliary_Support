#include "work.h"

#include <unistd.h>
#include <dirent.h>
using namespace std;


int process_task(int recv_struct[2],string pro)
{
	int start_index=recv_struct[0];

	//process
	char cmd[1024];
	sprintf(cmd,"%s %d",pro.c_str(), start_index);
	printf("++++++++++++++++++++++++%s %d",pro.c_str(), start_index);
	int re = system(cmd);
	printf("System return is [%d]!\n",re);
		
	
	return 0;

}
