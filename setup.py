
from distutils.core import setup, Extension

btree_ext = Extension('abtree_c', 
	sources = [
		'pywrap.cpp',
	        'abtree/serial.cpp',
                'abtree/file_bstore.cpp',
                'abtree/file_io.cpp',
                'abtree/io.cpp',
                'abtree/hilbert.c', 
		'tiny_boost/libs/python/src/converter/arg_to_python_base.cpp',
		'tiny_boost/libs/python/src/converter/builtin_converters.cpp',
		'tiny_boost/libs/python/src/converter/from_python.cpp',
		'tiny_boost/libs/python/src/converter/registry.cpp',
		'tiny_boost/libs/python/src/converter/type_id.cpp',
		'tiny_boost/libs/python/src/dict.cpp',
		'tiny_boost/libs/python/src/errors.cpp',
		'tiny_boost/libs/python/src/exec.cpp',
		'tiny_boost/libs/python/src/import.cpp',
		'tiny_boost/libs/python/src/list.cpp',
		'tiny_boost/libs/python/src/long.cpp',
		'tiny_boost/libs/python/src/module.cpp',
		'tiny_boost/libs/python/src/numeric.cpp',
		'tiny_boost/libs/python/src/object/class.cpp',
		'tiny_boost/libs/python/src/object/enum.cpp',
		'tiny_boost/libs/python/src/object/function.cpp',
		'tiny_boost/libs/python/src/object/function_doc_signature.cpp',
		'tiny_boost/libs/python/src/object/inheritance.cpp',
		'tiny_boost/libs/python/src/object/iterator.cpp',
		'tiny_boost/libs/python/src/object/life_support.cpp',
		'tiny_boost/libs/python/src/object/pickle_support.cpp',
		'tiny_boost/libs/python/src/object/stl_iterator.cpp',
		'tiny_boost/libs/python/src/object_operators.cpp',
		'tiny_boost/libs/python/src/object_protocol.cpp',
		'tiny_boost/libs/python/src/slice.cpp',
		'tiny_boost/libs/python/src/str.cpp',
		'tiny_boost/libs/python/src/tuple.cpp',
		'tiny_boost/libs/python/src/wrapper.cpp',
		'tiny_boost/libs/smart_ptr/src/sp_collector.cpp',
		'tiny_boost/libs/smart_ptr/src/sp_debug_hooks.cpp',
	],
	
	libraries=['pthread', 'm'],
	include_dirs=['tiny_boost', '.']
)

setup(
	name = 'abtree',
	version = '1.0.1',
	description = 'Make this reasonable',
	author = 'Jeremy Bruestle',
	author_email = 'jeremy.bruestle@gmail.com',
	package_dir = {'': 'python'},
	packages = ['abtree', 'stat_tree'],
	ext_modules = [btree_ext]
)

