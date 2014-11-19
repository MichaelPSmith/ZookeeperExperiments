//============================================================================
// Name        : ZKTest.cpp
// Author      : Michael P Smith (AKA, Krin), Under Codethink .Ltd
// Version     : 0.0.1
// Copyright   : Your copyright notice
// Description : a first attempt at making a zookeeper client in C
//============================================================================

#include <iostream>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "zookeeper.h"

using namespace std;

static zhandle_t *zk;
static const clientid_t *session_id;
int timeout = 3000;
int responseCode = 0;

void safeShutdown(zhandle_t *zzh);
void watcher(zhandle_t *zzh, int type, int state, const char *path,
		void *watcherCtx);

int main(int argc, char **argv)
{
	session_id = NULL;
	char* p;
	cout << "Initialising Zookeeper" << endl; // prints !!!Hello World!!!

	zk = zookeeper_init("localhost:2181", watcher, timeout, session_id, NULL, 0);
	while (!zk)
	{

	}
	p = strtok(NULL, " ");
	std::cout << "starting authentication" << std::endl;

	zoo_add_auth(zk, "digest", p, p ? strlen(p) : 0, NULL, NULL);
	while (zoo_state(zk) == 0)
	{

	}
	std::cout << "authentication step done" << std::endl;

	//responseCode = zoo_create(zk, "/test","my_data",7, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, NULL, NULL);
	while (zk)
	{

	}

	std::cout<<"why are we here?"<<std::endl;
	safeShutdown(zk);
	return 0;
}

///////////////////////////////////////////////////////////////////////
///                        END OF MAIN                              ///
///////////////////////////////////////////////////////////////////////

void safeShutdown(zhandle_t *zzh)
{
	if (zzh)
	{
		cout << "closing Zookeeper connection" << std::endl;
		zookeeper_close(zzh);
		zk = 0;
	}
}

void watcher(zhandle_t *zzh, int type, int state, const char *path,
		void *watcherCtx)
{
	if (type == ZOO_SESSION_EVENT)
	{
		if (state == ZOO_CONNECTED_STATE)
		{
			cout << "session Connected" << std::endl;
			zoo_create(zk, "/test","my_data",7, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, NULL, 0);
			zoo_create(zk, "/childTest","my_data",7, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
			zoo_exists(zk, "/test", true, NULL);
			/*zoo_exists tests if a node exists, and if the 3rd argument is non 0 sets a watch
			 * that watch is triggered upon any change to the file specified in arg 2
			 * argument 1 is the zookeeper session to make the request too
			 * */

			responseCode = zoo_get_children(zk, "/childTest", true, NULL);
			/*
			 * as zoo_exists but watches children of the specified node.
			 * argument 4 is a string that can be used to return the path of child nodes.
			 */

			std::cout<<responseCode<<std::endl;
			session_id = zoo_client_id(zzh);
		}
		else
		{
			std::cout<<"change detected!"<<std::endl;
		}
	}
	if (state == ZOO_AUTH_FAILED_STATE)
	{
		cout << "Refused connection WATCHER RESPONDED " << std::endl;
		;
		safeShutdown(zzh);
	}
	else if (state == ZOO_EXPIRED_SESSION_STATE)
	{
		cout << "session expired attempting to re-connect" << std::endl;
		zk = zookeeper_init("localhost:2181", watcher, timeout, session_id, NULL,
				0);
	}
	else if (state == ZOO_DELETED_EVENT)
	{
		cout << "node delete detected" << std::endl;
	}
	else if (state == ZOO_CHANGED_EVENT)
	{
		responseCode = zoo_exists(zk, path, true, NULL);
		cout << "change detected" << std::endl;
	}
	else if (state == ZOO_CHILD_EVENT)
	{
		zoo_get_children(zk, path, true, NULL);
		cout << "child change detected" << std::endl;
	}
}
