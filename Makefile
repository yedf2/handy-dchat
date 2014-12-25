include ../config.mk

SOURCES=dtest.cc proxy.cc

SHARED_SRCS=dcommon.cc

SHARED_OBJS=$(SHARED_SRCS:.cc=.o)

PROGRAMS = $(SOURCES:.cc=)

default: $(PROGRAMS)

$(PROGRAMS): $(LIBFILE) $(SHARED_OBJS)

clean:
	-rm -f $(PROGRAMS)
	-rm -f *.o

.cc.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

.cc:
	$(CXX) -o $@ $< $(SHARED_OBJS) $(CXXFLAGS) $(LDFLAGS)

