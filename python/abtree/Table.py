
import abtree_c
import collections

class Table(collections.MutableMapping):
	def __init__(self, inner, cmp_func, start, end):
		self.inner = inner
		self.cmp_func = cmp_func
		self.start = start
		self.end = end
	
	def __getitem__(self, key):
		if isinstance(key, slice):
			if key.step != None:
				raise TypeError("Step not allowed in slices")
			(new_start, new_end) = self.__intersect_range(key.start, key.stop)
			return Table(self.inner, self.cmp_func, new_start, new_end)
		elif self.__in_range(key):
			return self.inner.__getitem__(key)
		return None

	def __setitem__(self, key, value):
		if isinstance(key, slice):
			raise TypeError("Slice assignment not allowed")
		elif self.__in_range(key):
			self.inner.__setitem__(key, value)

	def __delitem__(self, key):
		if isinstance(key, slice):
			if key.step != None:
				raise TypeError("Step not allowed in slices")
			for x in self[key].keys():
				del self[x]
		elif self.__in_range(key):
			return self.inner.__delitem__(key)

	def __iter__(self):
		return self.inner.__iter__(self.start, self.end)

	def __len__(self): # TODO: This is broken for slices
		return self.inner.__len__()

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

	def total(self, base = None):
		return self.inner.total(self.start, self.end, base)

	def copy(self):
		return Table(self.inner.copy(), self.cmp_func, self.start, self.end)


