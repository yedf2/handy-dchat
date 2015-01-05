#include <handy-fw.h>
#include "dcommon.h"
#include <map>
#include <algorithm>
#include <limits>

using namespace std;
using namespace handy;

zhandle_t * zk_init(Conf& conf, EventBase* base, new_hash_table_cb cb);
void notify_new_hash_table(EventBase* base, string hashtable, long version) {
    static long last_version;
    if (last_version > version) {
        error("version %ld less than last version %ld", version, last_version);
        return;
    } else if (last_version == version) {
        info("version %ld equal to last version %ld", version, last_version);
        return;
    }
    replace(hashtable.begin(), hashtable.end(), '|', '\n');
    info("new hash table: \n%s\nlast_version: %ld -> version: %ld",
        hashtable.c_str(), last_version, version);
    last_version = version;
}


int main(int argc, const char* argv[]) {
    Conf conf = handy_app_init(argc, argv);

    int port = conf.getInteger("", "port", 0);
    exitif(port <= 0, "port should be positive interger");

    map<long, TcpConnPtr> users; //生命周期比连接更长，必须放在Base前
    EventBase base;

    zhandle_t* zh = zk_init(conf, &base, notify_new_hash_table);
    ExitCaller exit2([zh]{ zookeeper_close(zh); });

    Signal::signal(SIGINT, [&]{ base.exit(); });
    Signal::signal(SIGTERM, [&]{ base.exit(); });

    TcpServer chat(&base, "", port);
    chat.onConnCreate([&]{
        TcpConnPtr con(new TcpConn);
        con->setCodec(new LineCodec);
        con->onState([&](const TcpConnPtr& con) {
            if (con->getState() == TcpConn::Connected) {
                con->context<long>() = -1;
                const char* welcome = "#comment\n<user 101>#<user 102>#hello\n<user 101>#0#hello\n";
                con->send(welcome);
            } else if (con->getState() == TcpConn::Closed) {
                long id = con->context<long>();
                if (id > 0) {
                    users.erase(id);
                }
            }
        });
        con->onMsg([&](const TcpConnPtr& con, Slice msg){
            if (msg.size() == 0) { //忽略空消息
                return;
            }
            long& id = con->context<long>();
            ChatMsg cm(msg);
            info("handle msg. type: %d %s", cm.type, cm.str().c_str());
            if (cm.type == ChatMsg::Login) {
                if (id != -1) {
                    con->sendMsg("#login again. ignore");
                    //error("login again for conn: %s", con->peer_.toString().c_str());
                    return;
                }
                if (cm.fromId <= 0) {
                    //error("bad fromid for conn: %s", con->peer_.toString().c_str());
                    con->sendMsg("#bad fromid");
                    return;
                }
                id = cm.fromId;
                users[id] = con;
            } else if (cm.type == ChatMsg::Chat) {
                if (id == -1) {
                    con->sendMsg("#login first.");
                    //error("chat recv before login for conn: %s", con->peer_.toString().c_str());
                    return;
                }
                if (users.find(cm.toId) == users.end()) {
                    con->sendMsg("#user not found.");
                    return;
                } else {
                    users[cm.toId]->sendMsg(cm.str());
                }
            } else if (cm.type == ChatMsg::Logout) {
                con->close();
            }
        });
        return con;
    });
    base.loop();
    info("program exited");
    return 0;
}

