#include <conn.h>
#include <logging.h>
#include <daemon.h>
#include <dcommon.h>
using namespace std;
using namespace handy;

void dconnectTo(EventBase* base, const char* argv[], int uid, int touid) {
    TcpConnPtr con = TcpConn::createConnection(base, argv[1], atoi(argv[2]));
    con->setCodec(new LineCodec);
    con->onState([=] (const TcpConnPtr& con) {
        if (con->getState() == TcpConn::Connected) {
            ChatMsg loginMsg(ChatMsg::Login, 0, uid, 0, "");
            ChatMsg hello(ChatMsg::Chat, 1, uid, touid, "hello");
            con->sendMsg(loginMsg.str());
            con->sendMsg(hello.str());
        } else if (con->getState() == TcpConn::Failed) {
            error("connect failed");
            base->exit();
        } else if (con->getState() == TcpConn::Closed) {
            base->exit();
        }
    });
    con->onMsg([=](const TcpConnPtr& con, Slice msg){
        ChatMsg cmsg(msg);
        info("user %d msg: type %s %s", uid, cmsg.strType().c_str(), cmsg.str().c_str());
        if (cmsg.type == ChatMsg::Chat) {
            base->exit();
        }
    });
}

int main(int argc, const char* argv[]) {
    setloglevel("TRACE");
    if (argc < 3) {
        printf("usage: %s <host> <port>\n", argv[0]);
        return 1;
    }
    EventBase base;
    dconnectTo(&base, argv, 101, 102);
    dconnectTo(&base, argv, 102, 101);
    Signal::signal(SIGINT, [&]{base.exit();});
    base.loop();
    return 0;
}
