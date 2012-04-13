
from distutils.core import setup, Extension

btree_ext = Extension('btree_c', 
	sources = [
		'pywrap.cpp',
	],
	libraries=['abtree', 'boost_thread-mt', 'boost_python-mt']
)

setup(
	name = 'btree',
	version = '1.0.1',
	description = 'Make this reasonable',
	author = 'Jeremy Bruestle',
	author_email = 'jeremy.bruestle@gmail.com',
	packages = ['btree'],
	ext_modules = [btree_ext]
)

