#pragma once
#include <codec.h>
#include <util.h>
#include <string>
#include <zookeeper.h>
#include <handy.h>

struct ChatMsg {
    /*
    #...            comment message
    101#            user101 login
    1#101#102#hello   user101 -> user102 chat message
    ^D              logout
                    Empty
    */
    enum Type{ Unknow=0, Empty, Login, Comment, Logout, Chat, };
    enum User { System=0, Normal=100, };
    Type type;
    long msgId, fromId, toId;
    std::string data;
    std::string str();
    ChatMsg(Type type1, long msgId1, long fromId1, long toId1, handy::Slice data1):
        type(type1), msgId(msgId1), fromId(fromId1), toId(toId1), data(data1) {}
    ChatMsg():type(Unknow), fromId(0), toId(0) {}
    ChatMsg(handy::Slice line);
};

struct zkNode{
    std::string server;
    long id;
    zkNode():id(-1) { }
    int parse(handy::Slice ln);
};

bool zk_is_valid_server(handy::Slice ln);
void zk_node_set_cb(int rc, const struct Stat *stat, const void *data);
void zk_node_created(int rc, const char *value, const void *data);
void zk_create_node(zhandle_t * zh, const char* path, std::string val);
void zk_create_nodes(zhandle_t * zh, const char* path);

typedef void(*new_hash_table_cb)(handy::EventBase*, std::string, long);

