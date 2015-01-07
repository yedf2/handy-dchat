#include <conn.h>
#include <logging.h>
#include <daemon.h>
#include <dcommon.h>
using namespace std;
using namespace handy;

TcpConnPtr dconnectTo(EventBase* base, const char* argv[], int uid) {
    TcpConnPtr con = TcpConn::createConnection(base, argv[1], atoi(argv[2]));
    con->setCodec(new LineCodec);
    con->onState([=] (const TcpConnPtr& con) {
        if (con->getState() == TcpConn::Connected) {
            ChatMsg loginMsg(ChatMsg::Login, 0, uid, 0, "");
            info("sending msg %s to %s", loginMsg.str().c_str(), con->str().c_str());
            con->sendMsg(loginMsg.str());
        } else if (con->getState() == TcpConn::Failed) {
            error("connect failed");
            base->exit();
        } else if (con->getState() == TcpConn::Closed) {
            base->exit();
        }
    });
    return con;
}

int main(int argc, const char* argv[]) {
    setloglevel("TRACE");
    if (argc < 3) {
        printf("usage: %s <host> <port>\n", argv[0]);
        return 1;
    }
    EventBase base;
    TcpConnPtr con1 = dconnectTo(&base, argv, 101);
    TcpConnPtr con2 = dconnectTo(&base, argv, 102);
    auto msgcb = [&base](const TcpConnPtr& con, Slice msg) {
        ChatMsg cmsg(msg);
        info("%s handle msg: %s", con->str().c_str(), cmsg.str().c_str());
        if (cmsg.type == ChatMsg::Chat) {
            ChatMsg resp(ChatMsg::Ack, cmsg.msgId, cmsg.toId, cmsg.fromId, "ok");
            info("send msg: %s to %s", resp.str().c_str(), con->str().c_str());
            con->sendMsg(resp.str());
        } else if (cmsg.type == ChatMsg::Ack && cmsg.msgId == 111) {
            base.exit();
        }
    };
    con1->onMsg(msgcb);
    con2->onMsg(msgcb);
    base.runAfter(50, [con1]{
        ChatMsg hello(ChatMsg::Chat, 111, 101, 102, "hello");
        info("send msg %s to %s", hello.str().c_str(), con1->str().c_str());
        con1->sendMsg(hello.str());
    });
    Signal::signal(SIGINT, [&]{base.exit();});
    base.loop();
    return 0;
}
