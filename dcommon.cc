#include "dcommon.h"
#include <logging.h>
#include <vector>

using namespace std;

namespace handy {

string ChatMsg::str() {
    switch (type) {
        case Chat:
            return util::format("%ld#%ld#%s", fromId, toId, data.c_str());
        case Login:
            return util::format("%ld#", fromId);
        case Comment:
            return data;
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
    } else if (vs.size() == 3) {
        Slice s0 = vs[0], s1 = vs[1], s2 = vs[2];
        fromId = util::atoi2(s0.begin(), s0.end());
        toId = util::atoi2(s1.begin(), s1.end());
        if (fromId >= 0 && toId >= 0) {
            type = Chat;
            data = s2;
        }
    }
    if (type == Unknow) {
        error("unknow msg: %.*s", (int)line.size(), line.data());
    }
}

}