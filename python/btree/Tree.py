
import btree_c
import collections
import msgpack

class Tree(collections.MutableMapping):
	def __init__(self,  *args, **kwargs):
		if (len(args) == 0 and 'auto_inner' in kwargs):
			self.inner = kwargs['auto_inner']
		else:
			self.__init_normal__(*args, **kwargs)

	def __init_normal__(self, store, name, aggregate_func,
		cmp_func = cmp,
		serialize_func = msgpack.packb,
		deserialize_func = msgpack.unpackb,
		max_unwritten = 1000,
		max_lru = 10000,
		auto_inner=None):
		policy = {
			'max_unwritten' : max_unwritten,
			'max_lru' : max_lru,
			'name' : name,
			'serialize' : serialize_func,
			'deserialize' : deserialize_func,
			'cmp' : cmp_func,
			'aggregate' : aggregate_func
		}
		self.inner = btree_c.Tree(store, policy)
	
	def __getitem__(self, key):
		print "Doing getitem"
		return self.inner.__getitem__(key)

	def __setitem__(self, key, value):
		return self.inner.__setitem__(key, value)

	def __delitem__(self, key):
		return self.inner.__delitem__(key)

	def __iter__(self):
		return self.inner.__iter__()

	def __len__(self):
		return self.inner.__len__()

	def sync(self):
		return self.inner.sync()

	def copy(self):
		return Tree(auto_inner=self.inner.copy())


