#include <handy-fw.h>
#include "dcommon.h"
#include <map>
#include <algorithm>
#include <limits>

using namespace std;
using namespace handy;

set<int> hashBuckets;
long hashVersion;
string serverHost;
short port;

map<long, TcpConnPtr> allConns, userRouter;

zhandle_t * zk_init(Conf& conf, EventBase* base, new_hash_table_cb cb);

void notify_new_hash_table(EventBase* base, string hashtable, long version) {
    exitif(hashVersion > version, "version %ld less than last version %ld", version, hashVersion);
    if (hashVersion == version) {
        info("version %ld equal to last version %ld", version, hashVersion);
        return;
    }
    replace(hashtable.begin(), hashtable.end(), '|', '\n');
    info("new hash table: \n%s\nlast_version: %ld -> version: %ld",
        hashtable.c_str(), hashVersion, version);
    hashVersion = version;

    hashBuckets.clear();
    vector<Slice> buckets = Slice(hashtable).split('\n');
    for (auto bln: buckets) {
        vector<Slice> vs = bln.split(':');
        if (vs[0] == serverHost && util::atoi(vs[1].begin()) == port) {
            hashBuckets.insert(util::atoi(vs[2].begin()));
        }
    }
    string allbuckets;
    for (int buck: hashBuckets) {
        allbuckets += util::format("%d ", buck);
    }
    info("buckets for this router is %s", allbuckets.c_str());
    userRouter.clear();
    for (auto& p: allConns) {
        p.second->sendMsg(util::format("#%ld", hashVersion));
    }
}

int main(int argc, const char* argv[]) {
    Conf conf = handy_app_init(argc, argv);

    port = conf.getInteger("", "port", 0);
    exitif(port <= 0, "port should be positive interger");
    serverHost = conf.get("", "zk_server_host", "");
    exitif(serverHost.empty(), "zk_server_host should be set");

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
                string msg = util::format("#%ld", hashVersion);
                info("send %s to connection %ld", msg.c_str(), con->getChannel()->id());
                con->sendMsg(msg);
                allConns[con->getChannel()->id()] = con;
            } else if (con->getState() == TcpConn::Closed) {
                allConns.erase(con->getChannel()->id());
            }
        });
        con->onMsg([&](const TcpConnPtr& con, Slice msg){
            long& rversion = con->context<long>();
            if (msg.size() && msg[0] == '#') {
                long nver = util::atoi(msg.data()+1);
                if (nver <= rversion) {
                    error("bad new version %ld old version %ld", nver, rversion);
                    con->close();
                    return;
                }
                rversion = nver;
                return;
            }
            ChatMsg cm(msg);
            if (cm.type == ChatMsg::Unknow) {
                error("unknow msg: %.*s", (int)msg.size(), msg.data());
                con->close();
                return;
            }
            debug("%s handle msg. %s", con->str().c_str(), cm.str().c_str());
            ChatMsg ack(ChatMsg::Ack, cm.msgId, 0, cm.fromId, "");
            if (rversion != hashVersion) {
                ack.data = util::format("rversion %ld unmatch lversion %ld, retry later", rversion, hashVersion);
            } else if (cm.type == ChatMsg::Login) {
                userRouter[cm.fromId] = con;
                return;
            } else if (cm.type == ChatMsg::Chat || cm.type == ChatMsg::Ack) {
                auto rp = userRouter.find(cm.toId);
                if (rp == userRouter.end()) {
                    ack.data = "user not found";
                } else {
                    info("sending msg %s to %s", cm.str().c_str(), con->str().c_str());
                    rp->second->sendMsg(msg);
                    return;
                }
            } else if (cm.type == ChatMsg::Logout) {
                userRouter.erase(cm.fromId);
                return;
            }
            info("sending msg %s to %s", ack.str().c_str(), con->str().c_str());
            con->sendMsg(ack.str());
        });
        return con;
    });
    base.loop();
    info("program exited");
    return 0;
}

