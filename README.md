

If you are in a hurry to get started, see the [Python Quickstart](#python_quickstart) or the [C++ Quickstart](#cpp_quickstart) depending on your language of choice.

## What is an Aggregate B-tree
An Aggregate B-tree is an extension of the [B+ Tree](http://en.wikipedia.org/wiki/B%2B_tree).  The B+ Tree itself is powerful data structure that allows fast storage and lookup of key-value pairs, walking keys in order, or finding the nearest key, and forms the basis of most database indexes.  Unlike many B-tree implementions, this implementaion supports an idea called 'shadowing', which means that it's a trival operation to 'clone' the tree.  This allows one thread to modify a tree while another continues to see a stable version, or to keep many old versions with little overhead.  The code in this libary is thread safe, and crash safe.  Even if a process crashes in the middle of an operation, after it restarts, the tree will be in a stable state.  In addition, the abtree libaray allows multiple trees to be used in a transactional way, providing [ACID](http://en.wikipedia.org/wiki/ACID) guarantees (see section on [transactions](#transactions))

Aggregation adds another level of features.  Specifically, it's also possible to quickly compute 'aggregates', such as totals, maximums, minimums and averages over any range of keys.  It's also possible quickly move through the tree based on these aggregations.  Aggregations also allow more non-obvious abilities, such as putting ranges into a tree, and quickly finding all the overlapping ranges, quickly jumping of a specific number of records, seeing only 'high-priority' records, or even storing 3 dimensional points and quickly finding all of the points within an arbitrary box!  The feature of aggregation can also be used to implement [map-reduce](http://en.wikipedia.org/wiki/MapReduce) like functionality, and in fact, an aggregate b-tree is the basis of some of the features of [couchdb](http://couchdb.apache.org/).  While the current version of the library provides the direct raw interface needed to implement many of these specialized indexes, futures versions will attempt to provide wrappers to simplify common cases (such as spatial indexes).

Currently there are two independent API's to the libaray, a [C++ API](#cpp_api), and a [python API](#python_api).  Both are meant to be run from a single process (although multiple threads are supported), and both are allow for either disk based storages, or in memory storage.  Future versions supporting a distributed, fault tolerant version are in the works, but too early to meaningfully release.  When using disk based storage, the libary writes to a 'store', which is a set of files in any directory chosen by the user.  Multiple tree which are part of a transaction must be in the same store (see [transactions](#transactions) and [storage](#storage) for details).

# Using abtree in C++
<a name = "cpp_quickstart">
## C++ Quickstart
### Install boost
The abtree library uses the boost C++ libraries.  For most linux distro, boost can be installed using the package manager.  For example, in debian or ubuntu, you can simpley run `sudo apt-get install libboost-dev`.  For macports, it's simply `sudo port install boost`.  Since the abtree library only uses the header file from boost, not the actual libraries, you can also just untar a boost package from http://www.boost.org/ and copy the headers to your system include path.  
### Build and install abtree
````
git clone git@github.com:jbruestle/aggregate_btree.git
cd aggregate_btree
autoreconf -fi
mkdir build && cd build
../configure
make
sudo make install
````
### C++ Tutorial
The libary makes the aggregate btree look like an STL collection with some extra behaviors.  These is currently no documentation, but please see the examples in test/\*.cpp for a rough idea of the usage.

<a name = "cpp_api">
## C++ API

TODO: Write this

# Using abtree in Python
<a name = "python_quickstart">
## Python Quickstart
### Build and Install abtree

````
git clone git@github.com:jbruestle/aggregate_btree.git
cd aggregate_btree
sudo python ./setup.py install
````

### Python Tutorial

An abtree looks like a very special python dictionary, which is backed by some [files](#storage) on disk.  The simpliest use case would be as follows
````python
import abtree

store = abtree.Store("/some/dir")  # Define a storage location (create if needed)
tree = abtree.Tree(store)  # Load/create the tree in that location
````

This makes a tree, which acts just like a python dictionary (in fact it's a [MutableMapping](http://docs.python.org/library/collections.html#collections-abstract-base-classes)).  For example, you could do the following:
````python
tree["key"] ="value"  # Add/change a key/value pair
for x in tree.keys():  # Iterate over all the keys
    print x
````

<a name = "python_api">
## Python API

TODO: Write this

# Various details

<a name = "transactions">
## Transactions

<a name = "storage">
## Storage details
TODO: Write this
