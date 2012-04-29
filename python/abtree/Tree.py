
import abtree_c
import collections

class Tree(collections.MutableMapping):
	def __init__(self, inner):
		self.inner = inner
	
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
		return Tree(self.inner.copy())


