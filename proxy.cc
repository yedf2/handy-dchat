#include <handy-fw.h>
#include "dcommon.h"
#include <map>
#include <logging.h>
#include <zookeeper.h>

using namespace std;
using namespace handy;

int zoo_awget(zhandle_t *zh, const char *path,
                       watcher_fn watcher, void* watcherCtx,
                       data_completion_t completion, const void *data);
int zoo_aget(zhandle_t *zh, const char *path, int watch,
                                  data_completion_t completion, const void *data);

void zk_node_data(int rc, const char *value, int value_len, const struct Stat *stat, const void *data) {
    if (rc == 0) {
        EventBase* base = (EventBase*)data;
        string v(value, value_len);
        base->safeCall([v] { info("value %s from zk", v.c_str()); });
    } else {
        error("zk_node_data rc: %d value: %.*s", rc, value_len, value);
    }
}

void zk_node_watcher(zhandle_t *zh, int type, int state, const char *path, void *ctx) {
    int r = zoo_aget(zh, path, 1, zk_node_data, ctx);
    exitif(r, "zoo_aget failed");
}

void zk_init_watcher(zhandle_t * zh, int type, int state,const char *path, void *ctx) {
    info("zookeeper_init callback. type: %d state: %d", type, state);
    //info("states: %d %d %d %d %d %d", ZOO_CREATED_EVENT
    //    , ZOO_DELETED_EVENT, ZOO_CHANGED_EVENT, ZOO_CHILD_EVENT, ZOO_SESSION_EVENT, ZOO_NOTWATCHING_EVENT);
    if (type == ZOO_SESSION_EVENT || type == ZOO_CHANGED_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            info("connected to zookeeper successfully");
            auto arg = (pair<EventBase*, string>*)ctx;
            info("zoo_awget [%s]", arg->second.c_str());
            int r = zoo_awget(zh, arg->second.c_str(), zk_node_watcher, arg->first, zk_node_data, arg->first);
            exitif(r, "zoo_awget failed");
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            info("zookeeper session expired");
        }
    }
}
FILE* zk_log;
zhandle_t * zk_init(Conf& conf, EventBase* base, void* arg) {
    string zk_hosts = conf.get("", "zk_hosts", "");
    exitif(zk_hosts.empty(), "zk_hosts should be configured");
    FILE* zk_log = fdopen(Logger::getLogger().getFd(), "a");
    zoo_set_log_stream(zk_log);
    int loglevel = Logger::getLogger().getLogLevel();
    if (loglevel >= Logger::LDEBUG) {
        zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    } else {
        zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
    }
    zhandle_t *zh = zookeeper_init(zk_hosts.c_str(), zk_init_watcher,
        10*1000, 0, arg, 0);
    exitif(zh == NULL, "zookeeper_init failed");
    return zh;
}

int main(int argc, const char* argv[]) {
    Conf conf = handy_app_init(argc, argv);
    int port = conf.getInteger("", "port", 0);
    exitif(port <= 0, "port should be positive interger");

    map<long, TcpConnPtr> users; //生命周期比连接更长，必须放在Base前
    EventBase base;

    string zk_path = conf.get("", "zk_path", "");
    exitif(zk_path.empty(), "zk_path should be sed");
    auto arg = new pair<EventBase*, string>(&base, zk_path);
    zhandle_t* zh = zk_init(conf, &base, arg);
    ExitCaller exit2([zh, arg]{ zookeeper_close(zh); delete arg; arg=NULL; if (zk_log) fclose(zk_log);zk_log = NULL; });

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

