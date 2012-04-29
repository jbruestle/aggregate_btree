
import abtree_c
import collections
import json

class Tree(collections.MutableMapping):
	def __init__(self,  *args, **kwargs):
		if (len(args) == 0 and 'auto_inner' in kwargs):
			self.inner = kwargs['auto_inner']
		else:
			self.__init_normal__(*args, **kwargs)

	def __init_normal__(self, store, 
		name = "root", 
		aggregate_func = lambda a,b: None,
		cmp_func = cmp,
		serialize_func = json.dumps,
		deserialize_func = json.loads,
		auto_inner=None):
		policy = {
			'name' : name,
			'serialize' : serialize_func,
			'deserialize' : deserialize_func,
			'cmp' : cmp_func,
			'aggregate' : aggregate_func
		}
		self.inner = abtree_c.Tree(store.store, policy)
	
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

	def lower_bound(self, k):
		return self.inner.lower_bound(k)

	def upper_bound(self, k):
		return self.inner.upper_bound(k)

	def total(self, k1, k2):
		return self.inner.total(k1, k2)

	def commit(self):
		return self.inner.sync()

	def copy(self):
		return Tree(auto_inner=self.inner.copy())


