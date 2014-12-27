#pragma once
#include <codec.h>
#include <util.h>
#include <string>

namespace handy {

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
    ChatMsg(Type type1, long msgId1, long fromId1, long toId1, Slice data1):
        type(type1), msgId(msgId1), fromId(fromId1), toId(toId1), data(data1) {}
    ChatMsg():type(Unknow), fromId(0), toId(0) {}
    ChatMsg(Slice line);
};
}