env = Environment()
env.MergeFlags(env.ParseFlags('-O0 -g -std=c++11 -Wall -I. -I../handy -lhandy -L../handy -I/usr/include/zookeeper -lzookeeper_mt'))

env.Program('router', 'router.cc router-zk.cc dcommon.cc'.split())
env.Program('dtest', 'dtest.cc dcommon.cc'.split())
env.Program('proxy', 'proxy.cc proxy-zk.cc dcommon.cc'.split())

copyf = Builder(action='cp -f $SOURCE $TARGET')
env2 = Environment(BUILDERS={'Copyf':copyf})
env2.Copyf('router2', 'router')
env2.Copyf('router3', 'router')