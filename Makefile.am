
lib_LTLIBRARIES = libabtree.la
libabtree_la_SOURCES = \
		abtree/serial.cpp \
		abtree/file_bstore.cpp \
		abtree/file_io.cpp \
		abtree/io.cpp \
		abtree/hilbert.c 
libabtree_la_LIBADD = -lm -lpthread -lbz2 
abtree_includedir=$(includedir)
abtree_include_HEADERS = \
		abtree/disk_abtree.h \
		abtree/abtree.h \
		abtree/interval_tree.h \
		abtree/bcache.h \
		abtree/bdecl.h \
		abtree/biter.h \
		abtree/bnode.h \
		abtree/bnode_proxy.h \
		abtree/bnode_ptr.h \
		abtree/btree.h \
		abtree/file_bstore.h \
		abtree/file_io.h \
		abtree/io.h \
		abtree/serial.h \
		abtree/spatial.h \
		abtree/hilbert.h 

AM_CPPFLAGS = -I$(top_srcdir)/gtest-1.5.0/include -I$(top_srcdir)/gtest-1.5.0 -O3 -g
#AM_CPPFLAGS = -I$(top_srcdir)/gtest-1.5.0/include -I$(top_srcdir)/gtest-1.5.0 -g
bin_PROGRAMS = test_btree
test_btree_LDADD = libabtree.la -lm -lpthread -lbz2
test_btree_LDFLAGS = -g
test_btree_SOURCES = \
		gtest-1.5.0/src/gtest-all.cc \
		test/side_by_side.cpp \
		test/basic_memory.cpp \
		test/rolling_cleanup.cpp \
		test/interval_tree.cpp \
		test/walker.cpp \
		test/spatial.cpp \
		test/test_main.cpp
		
