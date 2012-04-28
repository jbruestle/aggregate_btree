

If you are in a hurry to get started, see the [Python Quickstart](#python_quickstart) or the [C++ Quickstart](#cpp_quickstart) depending on your language of choice.

# Using abtree in C++
## C++ Quickstart
<a name = "cpp_quickstart">
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

# Using abtree in Python
## Python Quickstart
### Build and Install abtree

````
git clone git@github.com:jbruestle/aggregate_btree.git
cd aggregate_btree
sudo python ./setup.py install
````

# Internal details
