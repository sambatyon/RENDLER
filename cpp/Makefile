CXX = g++
MESOS_SRC = /Users/alexander/Documents/workspace/pmesos
MESOS_BUILD = $(MESOS_SRC)/build
INCLUDES = -I"$(MESOS_BUILD)/3rdparty/libprocess/3rdparty/glog-0.3.3/src" \
					 -I"$(MESOS_BUILD)/include" \
					 -I"$(MESOS_SRC)/include" \
					 -I"$(MESOS_SRC)/3rdparty/libprocess/3rdparty/stout/include"
CXXFLAGS = -g -O0 -pthread -std=c++11
LDFLAGS += -L"$(MESOS_BUILD)/src/.libs" -lmesos -lpthread -lprotobuf
CXXCOMPILE = $(CXX) $(INCLUDES) $(CXXFLAGS) -c -o $@
CXXLINK = $(CXX) $(INCLUDES) $(CXXFLAGS) -o $@

default: all
all: rendler crawl_executor render_executor

HEADERS = rendler_helper.hpp


crawl_executor: crawl_executor.cpp $(HEADERS)
	$(CXXLINK) $<  $(LDFLAGS) -lboost_regex -lcurl

%: %.cpp $(HEADERS)
	$(CXXLINK) $< $(LDFLAGS)

check: crawl
	./crawl http://mesosphere.io/team/

clean:
	(rm -f core crawl_executor render_executor rendler)
