include ../config.mk

CXXFLAGS+=-DTHREADED -I/usr/include/zookeeper

LDFLAGS+=-lzookeeper_mt

SOURCES=dtest.cc proxy.cc router.cc

SHARED_SRCS=dcommon.cc

SHARED_OBJS=$(SHARED_SRCS:.cc=.o)

PROGRAMS = $(SOURCES:.cc=)

default: $(PROGRAMS) router2 router3

$(PROGRAMS): $(LIBFILE) $(SHARED_OBJS)

n:
	make clean
	make

clean:
	-rm -f $(PROGRAMS)
	-rm -f *.o

router2: router
	cp -f router router2

router3: router
	cp -f router router3

.cc.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

.cc:
	$(CXX) -o $@ $< $(SHARED_OBJS) $(CXXFLAGS) $(LDFLAGS)

