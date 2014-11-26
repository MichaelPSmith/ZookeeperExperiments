//============================================================================
// Name        : zkServerFileSetup.cpp
// Author      : Michael P Smith
// Version     :
// Copyright   : 
// Description : Hello World in C++, Ansi-style
//============================================================================

//============================================================================
// Name        : ZKTest.cpp
// Author      : Michael P Smith (AKA, Krin), Under Codethink .Ltd
// Version     : 0.0.1
// Copyright   : Your copyright notice
// Description : a first attempt at making a zookeeper client in C
//============================================================================

#include <iostream>
#include <cstring>
#include <string.h>
#include <sstream>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "zookeeper.h"

using namespace std;

static zhandle_t *zk;
static const clientid_t *session_id;
void watcher(zhandle_t *zzh, int type, int state, const char *path,
		void *watcherCtx);

int main(int argc, char **argv)
{
	session_id = NULL;
	char* p;
	cout << "Initialising Zookeeper" << endl; // prints !!!Hello World!!!

	zk = zookeeper_init("localhost:2181", watcher, 3000, session_id, NULL, 0);
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
	}
	cout << "End of setup Program" << std::endl;
	return 0;
} // End of Main

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
	if (type == ZOO_SESSION_EVENT)
	{
		std::cout << type2String(type) << std::endl;
		if (state == ZOO_CONNECTED_STATE)
		{
			std::cout << state2String(type) << std::endl;
			/*
			 * create a temporary zookeeper node that will vanish when this client disconnects,
			 *  useful for testing.
			 */
			zoo_create(zk, "/test", "my_data", 7, &ZOO_OPEN_ACL_UNSAFE,
					ZOO_EPHEMERAL, NULL, 0);

			zoo_create(zk, "/configTest", "top level configuration file", 28, &ZOO_OPEN_ACL_UNSAFE,
								0, NULL, 0);
			char* znodeData = "/configTest/typeOneNode;/typeOneNodeWatchingTest;/typeOneNodeWatchingTest/youShouldSeeThis;";
			zoo_create(zk, "/configTest/typeOneNode",znodeData, strlen(znodeData), &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);

			char* znodeData1 = "/configTest/typeTwoNode;/typeTwoNodeWatchingTest;/typeTwoNodeWatchingTest/youShouldSeeThis;/typeTwoNodeWatchingTest/youShouldSeeThis/AndThisAlso;";
			zoo_create(zk, "/configTest/typeTwoNode",znodeData1, strlen(znodeData1), &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);

			char* znodeData2 = "dataPlaceHolder";
			zoo_create(zk, "/typeOneNodeWatchingTest",znodeData2, strlen(znodeData2), &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);
			zoo_create(zk, "/typeTwoNodeWatchingTest",znodeData2, strlen(znodeData2), &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);
			zoo_create(zk, "/typeTwoNodeWatchingTest/youShouldSeeThis",znodeData2, strlen(znodeData2), &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);
			zoo_create(zk, "/typeTwoNodeWatchingTest/youShouldSeeThis/AndThisAlso",znodeData2, strlen(znodeData2), &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);
			zoo_create(zk, "/typeOneNodeWatchingTest/youShouldSeeThis",znodeData2, strlen(znodeData2), &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);

			safeShutdown(zk);
			session_id = zoo_client_id(zzh);
			return;
		}
		else if (state == ZOO_AUTH_FAILED_STATE)
		{
			std::cout << state2String(type) << std::endl;
			safeShutdown(zzh);
		}
		else if (state == ZOO_EXPIRED_SESSION_STATE)
		{
			std::cout << state2String(type) << std::endl;
			zk = zookeeper_init("localhost:2181", watcher, 3000, session_id,
					NULL, 0);
		}
	}
	else if (type == ZOO_DELETED_EVENT)
	{
		std::cout << type2String(type) << std::endl;
		zoo_exists(zk, path, true, NULL);
	}
	else if (type == ZOO_CHILD_EVENT)
	{
		std::cout << type2String(type) << std::endl;
	}
	else if (type == ZOO_CHANGED_EVENT)
	{
		zoo_exists(zk, path, true, NULL);
		std::cout << type2String(type) << std::endl;
	}
	else if (type == ZOO_CREATED_EVENT)
	{
		zoo_exists(zk, path, true, NULL);
		std::cout << type2String(type) << std::endl;
		cout << "creation of watched node detected" << std::endl;
	}
}
