#include <conn.h>
#include <logging.h>
#include <daemon.h>
#include "dcommon.h"
#include <map>

using namespace std;
using namespace handy;

int main(int argc, const char* argv[]) {
    setloglevel("TRACE");
    map<long, TcpConnPtr> users; //生命周期比连接更长，必须放在Base前
    EventBase base;
    Signal::signal(SIGINT, [&]{ base.exit(); });

    TcpServer chat(&base, "", 99);
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
            info("handle msg. type: %d fromid: %ld toid: %ld msg: %s",
                cm.type, cm.fromId, cm.toId, cm.data.c_str());
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

