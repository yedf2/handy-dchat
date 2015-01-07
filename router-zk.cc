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
static string g_zk_server_host;
static long g_zk_server_port;
static long g_zk_default_buckets;
static long g_zk_default_min_server;
static string g_zk_mynode; //name of the node created by myself
static zhandle_t* g_zk_handle;
static new_hash_table_cb g_zk_new_hash_table_cb;

struct HashNodeArg {
    vector<zkNode> nodes;
    long ver;
    long buckets;
    long min_server_count;
    HashNodeArg():ver(0), buckets(0), min_server_count(0) {}
};

int zk_hash_rebuild(HashNodeArg* arg, const string& old, string* res) {
    vector<Slice> odists = Slice(old).split('|');
    if (odists.empty()) {
        odists.resize(arg->buckets);
    }
    set<string> valid_hosts;
    for (auto& node: arg->nodes) {
        valid_hosts.insert(node.server);
    }
    vector<zkNode> ndists;
    for (auto& dist: odists) {
        zkNode ln;
        if (dist.empty()) {
            ln.id = ndists.size();
            ndists.push_back(ln);
        } else {
            int r = ln.parse(dist);
            if (r == 0) {
                if (valid_hosts.find(ln.server) == valid_hosts.end()) {
                    ln.server = "";
                }
                if (ln.id != (long)ndists.size()) {
                    error("bad id %ld for %s. id should be %ld. ignore this node",
                        ln.id, string(dist).c_str(), ndists.size());
                    ln.id = ndists.size();
                    ln.server = "";
                }
                ndists.push_back(ln);
            } else  {
                error("unknown line %s", string(dist).c_str());
                return -1;
            }
        }
    }
    map<string, int> host_max, host_cur;
    for (auto& vh: valid_hosts) {
        host_cur[vh] = 0;
    }
    int host_low = arg->buckets / valid_hosts.size();
    int host_high = host_low + 1;
    int host_high_n = arg->buckets % valid_hosts.size();
    int cur_high_n = 0;
    for (auto& ln: ndists) {
        if (ln.server.empty()) continue;
        int cn = host_cur[ln.server];
        if (cn >= host_high) {
            ln.server = "";
        } else if (cn == host_low) {
            if (cur_high_n < host_high_n) {
                cur_high_n ++;
                host_cur[ln.server] ++;
            } else {
                ln.server = "";
            }
        } else {
            host_cur[ln.server] ++;
        }
    }
    auto p = host_cur.begin();
    for (auto& ln: ndists) {
        if (ln.server.empty()) {
            while (p->second == host_high || (p->second == host_low && cur_high_n == host_high_n)) {
                ++p;
            }
            ln.server = p->first;
            p->second ++;
            if (p->second == host_high) cur_high_n ++;
        }
    }
    string all;
    for (auto& ln: ndists) {
        if (all.size()) all += '|';
        all += util::format("%s:%ld", ln.server.c_str(), ln.id);
    }
    bool same = all == old;
    if (same) {
        info("new hash table is same as old");
        return 1;
    }
    info("new hash table: %s", all.c_str());
    *res = all;
    return 0;
}

void zk_router_node_set_cb(int rc, const struct Stat *stat, const void *data) {
    info("node set rc: %d version: %d", rc, stat->version);
    exitif(rc, "zk_node_set_cb failed");
    string v = *(string*)data;
    delete (string*)data;
    int ver = stat->version;
    g_zk_base->safeCall([v, ver](){ g_zk_new_hash_table_cb(g_zk_base, v, ver);});
}

void zk_router_node_data(int rc, const char *value, int value_len, const struct Stat *stat, const void *data) {
    HashNodeArg* arg = (HashNodeArg*)data;
    unique_ptr<HashNodeArg> release1(arg);
    if (rc == 0) {
        info("router_node_data is: %.*s version %d", value_len, value, stat->version);
        if (value_len == 0) {
            string nodename = util::format("%s/version:1:buckets:%ld:min_server_count:%ld",
                g_zk_path.c_str(), g_zk_default_buckets, g_zk_default_min_server);
            zk_create_node(g_zk_handle, nodename.c_str(), "");
        }
        string cur(value, value_len);
        if (arg == NULL) { //only node change, we do not rebuild
            g_zk_new_hash_table_cb(g_zk_base, cur, stat->version);
            return;
        }
        string* res = new string;
        int r = zk_hash_rebuild(arg, cur, res);
        if (r == 0) { //rebuild ok
            r = zoo_aset(g_zk_handle, g_zk_path.c_str(), res->c_str(),
                (int)res->size(), stat->version, zk_router_node_set_cb, res);
            exitif(r, "zoo_aset failed");
        } else {
            delete res;
        }
    } else {
        exitif(1, "zk_router_node_data rc: %d value: %.*s", rc, value_len, value);
    }
}

bool zk_is_master(vector<zkNode>& nodes, zkNode& n) {
    for (size_t i = 0; i < nodes.size(); i ++) {
        if (nodes[i].id < n.id) {
            return false;
        }
    }
    return true;
}

void zk_children_data(int rc, const struct String_vector *strings, const void *data) {
    info("zk_children_data rc %d strings.size %d", rc, strings->count);
    exitif(rc, "zk_children_data failed");
    string all;
    HashNodeArg* arg = new HashNodeArg;
    for (int i = 0; i < strings->count; i ++) {
        vector<Slice> ws = Slice(strings->data[i]).split(':');
        if (ws.size() == 6) {
            long ver = util::atoi(ws[1].begin());
            long buckets = util::atoi(ws[3].begin());
            long min_server_count = util::atoi(ws[5].begin());
            if (ws[0] == Slice("version") && ver > arg->ver
                && ws[2] == Slice("buckets") && buckets > 0
                && ws[4] == Slice("min_server_count") && min_server_count > 0)
            {
                arg->ver = ver;
                arg->buckets = buckets;
                arg->min_server_count = min_server_count;
            } else {
                error("unexpected node: %s", strings->data[i]);
            }
        } else if (ws.size() == 3) {
            zkNode n;
            int r = n.parse(strings->data[i]);
            if (r == 0) {
                arg->nodes.push_back(n);
            } else {
                error("unexpected node: %s", strings->data[i]);
            }
        } else {
            error("unexpected node: %s", strings->data[i]);
        }
        all += strings->data[i];
        all += " ";
    }
    info("all children: %s", all.c_str());
    zkNode mynode;
    int r = mynode.parse(g_zk_mynode);
    exitif(r, "mynode bad format");
    if (zk_is_master(arg->nodes, mynode)) {
        int r = zoo_aget(g_zk_handle, g_zk_path.c_str(), 1, zk_router_node_data, arg);
        exitif(r, "zoo_aget failed");
    } else {
        delete arg;
    }
}

void zk_children_watcher(zhandle_t *zh, int type, int state, const char *path, void *ctx) {
    int r = zoo_aget_children(zh, path, 1, zk_children_data, ctx);
    exitif(r, "zoo_aget_children failed");
}

void zk_router_node_watcher(zhandle_t* zh, int type, int state, const char* path, void* ctx) {
    int r = zoo_aget(zh, path, 1, zk_router_node_data, NULL);
    exitif(r, "zoo_aget %s failed", path);
}

void zk_init_watcher(zhandle_t * zh, int type, int state,const char *path, void *ctx) {
    info("zookeeper_init callback. type: %d state: %d", type, state);
    //1 2 3 4 -1 -2
    // ZOO_CREATED_EVENT ZOO_DELETED_EVENT ZOO_CHANGED_EVENT ZOO_CHILD_EVENT ZOO_SESSION_EVENT ZOO_NOTWATCHING_EVENT
    if (type == ZOO_SESSION_EVENT || type == ZOO_CHILD_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            zk_create_nodes(zh, g_zk_path.c_str());
            string nodename = util::format("%s/%s:%ld:", g_zk_path.c_str(), g_zk_server_host.c_str(), g_zk_server_port);
            if (type == ZOO_SESSION_EVENT) {
                info("connected to zookeeper successfully");
                info("registering router. %s", nodename.c_str());
                int r = zoo_acreate(zh, nodename.c_str(), "", 0,
                    &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL|ZOO_SEQUENCE, zk_node_created, &g_zk_mynode); 
                exitif(r, "zoo_acreate failed");
            }
            int r = zoo_awget_children(zh, g_zk_path.c_str(), zk_children_watcher,
                NULL, zk_children_data, NULL);
            exitif(r, "zoo_awget_children failed");
            r = zoo_awget(zh, g_zk_path.c_str(), zk_router_node_watcher,
                NULL, zk_router_node_data, NULL);
            exitif(r, "zoo_awget failed");
        }
        exitif(state == ZOO_EXPIRED_SESSION_STATE, "session expired");
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
    g_zk_server_host = conf.get("", "zk_server_host", "");
    exitif(g_zk_server_host.empty(), "zk_server_host should be configured");
    g_zk_server_port = conf.getInteger("", "port", 0);
    exitif(g_zk_server_port<=0, "port should be positive");
    g_zk_default_buckets = conf.getInteger("", "zk_buckets", 101);
    exitif(g_zk_default_buckets < 1, "zk_buckets should be positive");
    g_zk_default_min_server = conf.getInteger("", "zk_min_server_count", 1);
    exitif(g_zk_default_min_server<1, "zk_min_server_count should be positive");
    g_zk_base = base;
    zoo_set_log_stream(g_zk_log);
    //int loglevel = Logger::getLogger().getLogLevel();
    //if (loglevel >= Logger::LDEBUG) {
    //    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    //} else {
    //    zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
    //}
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
    zhandle_t *zh = zookeeper_init(zk_hosts.c_str(), zk_init_watcher,
        10*1000, 0, NULL, 0);
    exitif(zh == NULL, "zookeeper_init failed");
    g_zk_handle = zh;
    g_zk_new_hash_table_cb = cb;
    return zh;
}
