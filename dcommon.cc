#include "dcommon.h"
#include <logging.h>
#include <vector>

using namespace std;
using namespace handy;

string ChatMsg::str() {
    return util::format("%d#%ld#%ld#%ld#%s", type, msgId, fromId, toId, data.c_str());
}

ChatMsg::ChatMsg(Slice line): ChatMsg() {
    type = Unknow;
    vector<Slice> vs = line.split('#');
    if (vs.size() == 5) {
        type = (Type)util::atoi(vs[0].begin());
        msgId = util::atoi(vs[1].begin());
        fromId = util::atoi(vs[2].begin());
        toId = util::atoi(vs[3].begin());
        data = vs[4];
    }
    if (type == Unknow) {
        error("unknow msg: %.*s", (int)line.size(), line.data());
    }
}

int zkNode::parse(Slice ln) {
    vector<Slice> nodes = ln.split(':');
    if (nodes.size() != 3) {
        error("bad node format: %s", string(ln).c_str());
        return -1;
    }
    server = string(ln.begin(), ln.size()-nodes[2].size()-1);
    id = util::atoi2(nodes[2].begin(), nodes[2].end());
    if (id < 0 || !zk_is_valid_server(server)) {
        error("bad node format: %s", string(ln).c_str());
        return EINVAL;
    }
    return 0;
}

bool zk_is_valid_server(Slice ln) {
    vector<Slice> vs = ln.split(':');
    if (vs.size() != 2 || util::atoi2(vs[1].begin(), vs[1].end()) <= 0) {
        return false;
    }
    return true;
}

void zk_node_set_cb(int rc, const struct Stat *stat, const void *data) {
    info("node set rc: %d version: %d", rc, stat->version);
    exitif(rc, "zk_node_set_cb failed");
}

void zk_node_created(int rc, const char *value, const void *data) {
    if (rc == ZNODEEXISTS) {
        info("node %s already exists", value);
    } else if (rc == 0) {
        info("node %s created", value);
        if (data) {
            *(string*)data = value;
        }
    } else {
        exitif(1, "in zk_node_created. create failed %d %s", rc, value);
    }
}

void zk_create_node(zhandle_t * zh, const char* path, string val) {
    info("creating node %s", path);
    int r = zoo_acreate(zh, path, val.c_str(), val.size(),
        &ZOO_OPEN_ACL_UNSAFE, 0, zk_node_created, NULL);
    exitif(r, "zoo_acreate %s failed", path);
}

void zk_create_nodes(zhandle_t * zh, const char* path) {
    auto nodes = Slice(path).split('/');
    string cpath = "";
    for (size_t i = 1; i < nodes.size(); i ++) {
        cpath += "/";
        cpath += nodes[i];
        zk_create_node(zh, cpath.c_str(), "");
    }
}

