
lib_LTLIBRARIES = libabtree.la
libabtree_la_SOURCES = \
		abtree/serial.cpp \
		abtree/file_bstore.cpp \
		abtree/file_io.cpp \
		abtree/io.cpp 
libabtree_la_LIBADD = -lm -lboost_thread-mt
abtree_includedir=$(includedir)
nobase_abtree_include_HEADERS = \
		abtree.h \
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
		abtree/vector_io.h 

AM_CPPFLAGS = -I$(top_srcdir)/src
bin_PROGRAMS = test_btree
test_btree_LDADD = libabtree.la -lm -lboost_thread-mt
test_btree_SOURCES = \
		test/test_btree.cpp
