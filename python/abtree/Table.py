
import abtree_c
import collections
import _SeqView

class _Table(collections.MutableMapping):
	def __init__(self, inner, cmp_func, empty_total, start, end):
		self.inner = inner
		self.cmp_func = cmp_func
		self.empty_total = empty_total
		self.start = start
		self.end = end
	
	def __getitem__(self, key):
		if isinstance(key, slice):
			if key.step != None:
				raise TypeError("Step not allowed in slices")
			(new_start, new_end) = self.__intersect_range(key.start, key.stop)
			return _Table(self.inner, self.cmp_func, self.empty_total, new_start, new_end)
		elif self.__in_range(key):
			return self.inner.getitem(key)
		return None

	def __setitem__(self, key, value):
		if isinstance(key, slice):
			raise TypeError("Slice assignment not allowed")
		elif self.__in_range(key):
			self.inner.setitem(key, value)

	def __delitem__(self, key):
		if isinstance(key, slice):
			if key.step != None:
				raise TypeError("Step not allowed in slices")
			for x in self[key].keys():
				del self[x]
		elif self.__in_range(key):
			return self.inner.delitem(key)

	def __iter__(self):
		return self.inner.iter(self.start, self.end)

	def __len__(self): 
		return self.inner.len(self.start, self.end)

	def __in_range(self, key):
		if self.start != None and self.cmp_func(self.start, key) > 0:
			return False
		if self.end != None and self.cmp_func(key, self.end) > 0:
			return False
		return True

	def __intersect_range(self, new_start, new_end):
		result_start = new_start
		if new_start == None:
			result_start = self.start
		elif self.start != None and self.cmp_func(new_start, self.start) < 0:
			result_start = self.start
		result_end = new_end
		if new_end == None:
			result_end = self.end
		elif self.end != None and self.cmp_func(new_end, self.end) > 0:
			result_start = self.end
		return (new_start, new_end)

	def lower_bound(self, key):
		it = self.inner.iter(key, None)
		try:
			return it.next()
		except StopIteration:
			pass
		return None

	def total(self, base = None):
		if base == None:
			base = self.empty_total
		return self.inner.total(self.start, self.end, base)
	
	def key_at_index(self, index):
		return self.inner.key_at_index(self.start, self.end, index)

	def copy(self):
		return _Table(self.inner.copy(), self.cmp_func, self.empty_total, self.start, self.end)

	def keys(self):
		return _SeqView._SeqView(self, lambda k,v: k, 1)

	def values(self):
		return _SeqView._SeqView(self, lambda k,v: v, 1)

	def items(self):
		return _SeqView._SeqView(self, lambda k,v: (k,v), 1)

	def find(self, func):
		return self.inner.find(func)

	def aggregate_until(self, func):
		return self.inner.aggregate_until(self.start, self.end, func) 

def Table(aggregate_func = lambda a,b: None, empty_total = None, cmp_func = cmp):
	policy = {
		'cmp' : cmp_func,
		'aggregate' : aggregate_func
	}
	return _Table(abtree_c.MemoryTree(policy), cmp_func, empty_total, None, None)


