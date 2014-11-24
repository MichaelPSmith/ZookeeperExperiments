//============================================================================
// Name        : ZKTest.cpp
// Author      : Michael P Smith (AKA, Krin), Under Codethink .Ltd
// Version     : 0.0.1
// Copyright   : Your copyright notice
// Description : a first attempt at making a zookeeper client in C
//============================================================================

#include <iostream>
#include <thread>
#include <mutex>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "zookeeper.h"

using namespace std;

static zhandle_t *zk;
static const clientid_t *session_id;
struct String_vector list_of_children = {0};
int timeout = 3000;
int responseCode = 0;
std::mutex mutex_lock;

void safeShutdown(zhandle_t *zzh);
void watcher(zhandle_t *zzh, int type, int state, const char *path,
		void *watcherCtx);
void discoverChildren(const char* path);

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

	while (zk)
	{
		/*
		 * mutex used to make the main program loop wait until there is feedback
		 * from zookeeper. initially locks, then loops around. thread cannot lock
		 * again until the current lock has been unlocked.
		 */
		mutex_lock.lock();

		/*
		 * debug test to show that the loop does not continue until watcher
		 *  is called
		 */
		std::cout<<"loop locked"<<std::endl;
	}

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


static const char* state2String(int state)
{
	if (state == 0)
		return "CLOSED_STATE";
	if (state == ZOO_CONNECTING_STATE)
		return "CONNECTING_STATE";
	if (state == ZOO_ASSOCIATING_STATE)
		return "ASSOCIATING_STATE";
	if (state == ZOO_CONNECTED_STATE)
		return "CONNECTED_STATE";
	if (state == ZOO_EXPIRED_SESSION_STATE)
		return "EXPIRED_SESSION_STATE";
	if (state == ZOO_AUTH_FAILED_STATE)
		return "AUTH_FAILED_STATE";

	return "INVALID_STATE";
}

static const char* type2String(int type)
{
	if (type == ZOO_CREATED_EVENT)
		return "CREATED_EVENT";
	if (type == ZOO_DELETED_EVENT)
		return "DELETED_EVENT";
	if (type == ZOO_CHANGED_EVENT)
		return "CHANGED_EVENT";
	if (type == ZOO_CHILD_EVENT)
		return "CHILD_EVENT";
	if (type == ZOO_SESSION_EVENT)
		return "SESSION_EVENT";
	if (type == ZOO_NOTWATCHING_EVENT)
		return "NOTWATCHING_EVENT";

	return "UNKNOWN_EVENT_TYPE";
}
void watcher(zhandle_t *zzh, int type, int state, const char *path,
		void *watcherCtx)
{
	mutex_lock.unlock();
	if (type == ZOO_SESSION_EVENT)
	{
		std::cout<<type2String(type)<<std::endl;
		if (state == ZOO_CONNECTED_STATE)
		{
			std::cout<<state2String(type)<<std::endl;
			zoo_create(zk, "/test","my_data",7, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, NULL, 0);
			zoo_create(zk, "/childTest","my_data",7, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
			zoo_exists(zk, "/test", true, NULL);
			/*zoo_exists tests if a node exists, and if the 3rd argument is non 0 sets a watch
			 * that watch is triggered upon any change to the file specified in arg 2
			 * argument 1 is the zookeeper session to make the request too
			 * */

			responseCode = zoo_get_children(zk, "/childTest", true, NULL);
			discoverChildren("/childTest");
			/*
			 * as zoo_exists but watches children of the specified node.
			 * argument 4 is a string that can be used to return the path of child nodes.
			 */
			session_id = zoo_client_id(zzh);
			return;
		}
		else if (state == ZOO_AUTH_FAILED_STATE)
		{
			std::cout<<state2String(type)<<std::endl;
			safeShutdown(zzh);
		}
		else if (state == ZOO_EXPIRED_SESSION_STATE)
		{
			std::cout<<state2String(type)<<std::endl;
			zk = zookeeper_init("localhost:2181", watcher, timeout, session_id, NULL,
					0);
		}
	}
	else if (type == ZOO_DELETED_EVENT)
	{
		std::cout<<type2String(type)<<std::endl;
		zoo_exists(zk, path, true, NULL);
	}
	else if (type == ZOO_CHILD_EVENT)
	{
		std::cout<<type2String(type)<<std::endl;
		discoverChildren(path);
	}
	else if (type == ZOO_CHANGED_EVENT)
	{
		responseCode = zoo_exists(zk, path, true, NULL);
		std::cout<<type2String(type)<<std::endl;
	}
	else if (type == ZOO_CREATED_EVENT)
	{
		responseCode = zoo_exists(zk, path, true, NULL);
		std::cout<<type2String(type)<<std::endl;
		cout << "creation of watched node detected" << std::endl;
	}
}

void discoverChildren(const char* path)
{
	struct String_vector list_of_children_discovered = {0};
	cout << "discovering children"<<path<< std::endl;
	zoo_get_children(zk, path, true, &list_of_children_discovered);
	zoo_exists(zk, path, true, NULL);
	if(list_of_children_discovered.count)
	{
		for (int i = 0; i < list_of_children_discovered.count; i++)
		{
			string child_to_add = path;
			child_to_add += '/';
			child_to_add += list_of_children_discovered.data[i];
			const char* end_result = child_to_add.c_str();
			discoverChildren(end_result);
		}
		deallocate_String_vector(&list_of_children_discovered);
	}
}
