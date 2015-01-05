#include <handy-fw.h>
#include "dcommon.h"
#include <map>
#include <logging.h>
#include <zookeeper.h>

using namespace std;
using namespace handy;

long hashVersion;
vector<TcpConnPtr> bucketCons;
map<string, TcpConnPtr> addrCons;

TcpConnPtr getConByUid(long uid) {
    if (bucketCons.size() == 0) { return NULL; }
    int bucket = uid % bucketCons.size();
    return bucketCons[bucket];
}

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

    vector<Slice> lns = Slice(hashtable).split('\n');
    exitif(bucketCons.size() && bucketCons.size() != lns.size(), "hash table size changed");

    bucketCons.clear();
    vector<string> addrs;
    set<string> seta;
    map<string, TcpConnPtr> oldcons;
    swap(oldcons, addrCons);
    for (auto ln: lns) {
        zkNode nd;
        int r = nd.parse(ln);
        exitif(r || nd.id != (int)addrs.size(), "bad hash table");
        addrs.push_back(nd.server);
        seta.insert(nd.server);
        auto& con = addrCons[nd.server];
        con = oldcons[nd.server];
        bucketCons.push_back(con);
    }
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
            } else if (con->getState() == TcpConn::Closed) {
                long id = con->context<long>();
                if (id > 0) {
                    users.erase(id);
                }
            }
        });
        con->onMsg([&](const TcpConnPtr& con, Slice msg){
            long& id = con->context<long>();
            ChatMsg cm(msg);
            ChatMsg resp(ChatMsg::Ack, cm.msgId, cm.toId, cm.fromId, "");
            info("handle msg: %s", cm.str().c_str());
            if (cm.type == ChatMsg::Login) {
                exitif(id != -1 || cm.fromId <= 0, "login error");
                id = cm.fromId;
                users[id] = con;
                info("user %ld login ok", id);
                resp.data = "login ok";
            } else if (cm.type == ChatMsg::Chat) {
                exitif(id < 0, "msg recved before login");
                if (users.find(cm.toId) == users.end()) {
                    resp.data = "user not found";
                } else {
                    users[cm.toId]->sendMsg(cm.str());
                    return;
                }
            } else if (cm.type == ChatMsg::Logout) {
                con->close();
                return;
            }
            con->sendMsg(resp.str());
        });
        return con;
    });
    base.loop();
    info("program exited");
    return 0;
}

