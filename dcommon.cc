#include "dcommon.h"
#include <logging.h>
#include <vector>

using namespace std;

namespace handy {

string ChatMsg::str() {
    switch (type) {
        case Chat:
            return util::format("%ld#%ld#%ld#%s", msgId, fromId, toId, data.c_str());
        case Login:
            return util::format("%ld#", fromId);
        case Comment:
            return "#"+data;
        case Unknow:
        case Empty:
        case Logout:
        default:
            return "";
    }
}

ChatMsg::ChatMsg(Slice line): ChatMsg() {
    type = Unknow;
    vector<Slice> vs = line.split('#');
    if (vs.size() == 0) {
        type = Empty;
    } else if (vs.size() == 1) {
        Slice s0 = vs[0];
        if (s0.size() == 1 && s0[0] == 0x04) {
            type = Logout;
        }
    } else if (vs.size() == 2) {
        Slice s0 = vs[0], s1 = vs[1];
        if (s0.size() == 0) {
            type = Comment;
            data = s1;
        } else if (s1.size() == 0) {
            fromId = util::atoi2(s0.begin(), s0.end());
            type = fromId >= 0 ? Login : Unknow;
        }
    } else if (vs.size() == 4) {
        Slice s0 = vs[0], s1 = vs[1], s2 = vs[2], s3=vs[3];
        msgId = util::atoi2(s0.begin(), s0.end());
        fromId = util::atoi2(s1.begin(), s1.end());
        toId = util::atoi2(s2.begin(), s2.end());
        if (fromId >= 0 && toId >= 0) {
            type = Chat;
            data = s3;
        }
    }
    if (type == Unknow) {
        error("unknow msg: %.*s", (int)line.size(), line.data());
    }
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

}