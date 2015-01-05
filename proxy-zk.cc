#include "dcommon.h"
#include <zookeeper.h>
#include <conf.h>
#include <handy.h>
#include <logging.h>

using namespace std;
using namespace handy;

//zk functions
static FILE* g_zk_log;
static EventBase* g_zk_base;
static string g_zk_path;
static new_hash_table_cb g_zk_new_hash_table_cb;

void zk_node_data(int rc, const char *value, int value_len, const struct Stat *stat, const void *data) {
    if (rc == 0) {
        string v(value, value_len);
        int ver = stat->version;
        g_zk_base->safeCall([v, ver](){ g_zk_new_hash_table_cb(g_zk_base, v, ver);});
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
            info("zoo_awget [%s]", g_zk_path.c_str());
            int r = zoo_awget(zh, g_zk_path.c_str(), zk_node_watcher, g_zk_base, zk_node_data, g_zk_base);
            exitif(r, "zoo_awget failed");
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            info("zookeeper session expired");
        }
    }
}

zhandle_t * zk_init(Conf& conf, EventBase* base, new_hash_table_cb cb) {
    string zk_hosts = conf.get("", "zk_hosts", "");
    exitif(zk_hosts.empty(), "zk_hosts should be configured");
    g_zk_log = fdopen(Logger::getLogger().getFd(), "a");
    g_zk_path = conf.get("", "zk_path", "");
    if (g_zk_path.size() && g_zk_path[g_zk_path.size()-1] == '/') {
        g_zk_path = g_zk_path.substr(0, g_zk_path.size()-1);
    }
    exitif(g_zk_path.empty(), "zk_path should be set");
    g_zk_base = base;
    zoo_set_log_stream(g_zk_log);
    int loglevel = Logger::getLogger().getLogLevel();
    if (loglevel >= Logger::LDEBUG) {
        zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    } else {
        zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
    }
    zhandle_t *zh = zookeeper_init(zk_hosts.c_str(), zk_init_watcher,
        10*1000, 0, NULL, 0);
    exitif(zh == NULL, "zookeeper_init failed");
    g_zk_new_hash_table_cb = cb;
    return zh;
}

