//============================================================================
// Name        : ZKTest.cpp
// Author      : Michael P Smith (AKA, Krin), Under Codethink .Ltd
// Version     : 0.0.1
// Copyright   : Your copyright notice
// Description : a first attempt at making a zookeeper client in C
//============================================================================

#include <iostream>
#include <cstring>
#include <algorithm>
#include <thread>
#include <vector>
#include <mutex>
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
char* nodeType = "default";
struct String_vector list_of_children = {0};
int timeout = 3000;
int responseCode = 0;
std::mutex mutex_lock;
void safeShutdown(zhandle_t *zzh);
void configure();
void setConfiguration(char* configString);
void configurationwatcher(zhandle_t *zzh, int type, int state, const char *path,
		void *watcherCtx);
void watcher(zhandle_t *zzh, int type, int state, const char *path,
		void *watcherCtx);
void discoverChildren(const char* path);

int main(int argc, char **argv)
{
	/*
	 * checking to see if the host entered as the second argument is valid
	 * if no host is entered the program will default to local host
	 */
	char* hosts = "localhost:2181";
	char* check = "localhost";
	char* check2 = ":";
	if(argc >1)
	{
		for (int i = 1; i < argc; i++ )
		{
			if(isdigit(argv[i][0]))
			{
				if(strstr(argv[i],check2) != NULL)
				{
					hosts = argv[i];
				}
			}
			else if(strstr(argv[i],check) !=NULL)
			{
				if(strstr(argv[i],check2) != NULL)
				{
					hosts = argv[1];
				}

			}
			else if(isalpha(argv[i][0]))
			{
				nodeType = argv[i];
			}
		}
	}
	else
	{
		hosts = "localhost:2181";
	}

	session_id = NULL;
	char* p;
	cout << "Initialising Zookeeper" << endl; // prints !!!Hello World!!!

	zk = zookeeper_init(hosts, watcher, timeout, session_id, NULL, 0);
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
}// End of Main



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
			/*
			 * create a temporary zookeeper node that will vanish when this client disconnects,
			 *  useful for testing.
			 */
			zoo_create(zk, "/test","my_data",7, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, NULL, 0);

			/*
			 * Setting configuration to watch. taking the char[] provided by argv and appending it to a
			 * string containing configuration root directory before turning back into a char[] with
			 * the c_str() function to allow zookeeper to read the full address to watch
			 */
			stringstream ss;
			string temp;
			ss << nodeType;
			ss >> temp;
			string addition_of_strings = "/configTest/" + temp;
			const char* config_to_watch = addition_of_strings.c_str();
			std::cout<<"setting watch for config type of "<<config_to_watch<<std::endl;

			if(zoo_wexists(zk, config_to_watch ,configurationwatcher, NULL , NULL)==0)
			{
				char config_data[1024] = {0};
				int data_length = sizeof(config_data);
				zoo_get(zk, config_to_watch, true, config_data, &data_length, NULL);
				setConfiguration(config_data);
			}


			//responseCode = zoo_get_children(zk, "/childTest", true, NULL);
			//discoverChildren("/childTest");
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
	cout << "discovering children of "<<path<< std::endl;
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

void configurationwatcher(zhandle_t *zzh, int type, int state, const char *path,
		void *watchContext)
{
	char config_data[1049000] = {0};
	int data_length = sizeof(config_data);
	zoo_get(zk, path, true, config_data, &data_length, NULL);
	std::cout<<"i am a client of type "<<config_data<<std::endl;
	setConfiguration(config_data);
	zoo_wexists(zk, path, configurationwatcher, NULL , NULL);
}

void setConfiguration(char* configString)
{
	vector<char*> elements;
	uint configBegin = 0;
	for (uint it = 0; it != strlen(configString); it++ )
	{
		if(configString[it] == ';' || NULL)
		{
			string tempString;
			while(configBegin != it)
			{
				tempString += configString[configBegin];
				configBegin++;
			}
			configBegin = it+1;
			char* element = new char[tempString.length()+1];
			std::strcpy (element, tempString.c_str());
			elements.push_back(element);
		}
	}
	std::cout<<"i am a client of type "<<nodeType<<std::endl;
	std::cout<<"and i should watch the nodes:"<<std::endl;
	for (uint it = 0; it != elements.size(); it++)
	{
		std::cout<<elements[it]<<std::endl;
		zoo_exists(zk, elements[it], true, NULL);
		discoverChildren(elements[it]);
	}
}
