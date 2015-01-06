#include <handy-fw.h>
#include "dcommon.h"
#include <map>
#include <logging.h>
#include <zookeeper.h>

using namespace std;
using namespace handy;

long hashVersion;
vector<string> bucketAddrs;
map<string, TcpConnPtr> addrCons;
map<long, TcpConnPtr> userConns;

TcpConnPtr getConByUid(long uid) {
    if (bucketAddrs.size() == 0) { return NULL; }
    int bucket = uid % bucketAddrs.size();
    return addrCons[bucketAddrs[bucket]];
}

void versionMatched(TcpConnPtr con, string server) {
    for (auto& uc: userConns) {
        int bucket = uc.first % bucketAddrs.size();
        if (bucketAddrs[bucket] == server) {
            ChatMsg login(ChatMsg::Login, 0, uc.first, 0, "");
            con->sendMsg(login.str());
        }
    }
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
    exitif(bucketAddrs.size() && bucketAddrs.size() != lns.size(), "hash table size changed");

    bucketAddrs.clear();
    set<string> seta;
    map<string, TcpConnPtr> oldcons;
    swap(oldcons, addrCons);
    for (auto ln: lns) {
        zkNode nd;
        int r = nd.parse(ln);
        exitif(r || nd.id != (int)bucketAddrs.size(), "bad hash table");
        bucketAddrs.push_back(nd.server);
        seta.insert(nd.server);
        auto& con = addrCons[nd.server] = oldcons[nd.server];
        if (con->context<long>() == hashVersion) {
            versionMatched(con, nd.server);
        }
    }
}

struct RouterCtx {
    long version;
    string server;
};

void reconnectRouter(EventBase* base) {
    for (auto& pr: addrCons) {
        auto& con = pr.second;
        if (!con) {
            vector<Slice> vs = Slice(pr.first).split(':');
            string host = vs[0];
            short port = (short)util::atoi(vs[1].data());
            con = TcpConn::createConnection(base, host, port, 3);
            con->setCodec(new LineCodec);
            string server(pr.first);
            con->context<RouterCtx>().server = server;
            con->onState([](TcpConnPtr con) {
                if (con->getState() == TcpConn::Closed || con->getState() == TcpConn::Failed) {
                    string& server = con->context<RouterCtx>().server;
                    addrCons[server] = NULL;
                }
            });
            con->onMsg([server](TcpConnPtr con, Slice msg) {
                long& rversion = con->context<RouterCtx>().version;
                if (msg.size() && msg[0] == '#') {
                    long ver = util::atoi(msg.data()+1);
                    exitif(rversion != 0 && ver != rversion+1, "version not continous");
                    rversion = ver;
                    if (rversion == hashVersion) {
                        versionMatched(con, server);
                    }
                    return;
                }
                ChatMsg chat(msg);
                if (chat.type == ChatMsg::Chat) {
                    auto p = userConns.find(chat.toId);
                    if (p == userConns.end()) {
                        ChatMsg res(ChatMsg::Ack, chat.msgId, 0, chat.fromId, "user not found");
                        con->sendMsg(res.str());
                    } else {
                        p->second->sendMsg(msg);
                    }
                } else {
                    error("unexpected msg: %.*s", (int)msg.size(), msg.data());
                }
            });
        }
    }
}

int main(int argc, const char* argv[]) {
    Conf conf = handy_app_init(argc, argv);
    int port = conf.getInteger("", "port", 0);
    exitif(port <= 0, "port should be positive interger");

    EventBase base;

    zhandle_t* zh = zk_init(conf, &base, notify_new_hash_table);
    ExitCaller exit2([zh]{ zookeeper_close(zh); });

    Signal::signal(SIGINT, [&]{ base.exit(); });
    Signal::signal(SIGTERM, [&]{ base.exit(); });

    base.runAfter(0, [&base]{ reconnectRouter(&base); }, 3000);
    TcpServer chat(&base, "", port);
    chat.onConnCreate([&]{
        TcpConnPtr con(new TcpConn);
        con->setCodec(new LineCodec);
        con->onState([&](const TcpConnPtr& con) {
            if (con->getState() == TcpConn::Closed) {
                long id = con->context<long>();
                if (id > 0) {
                    userConns.erase(id);
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
                userConns[id] = con;
                info("user %ld login ok", id);
                resp.data = "login ok";
            } else if (cm.type == ChatMsg::Chat) {
                exitif(id < 0, "msg recved before login");
                if (userConns.find(cm.toId) == userConns.end()) {
                    resp.data = "user not found";
                } else {
                    userConns[cm.toId]->sendMsg(cm.str());
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

