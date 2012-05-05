
import collections

class _SeqView(collections.Sequence):
	def __init__(self, table, item_func, step):
		self.table = table
		self.item_func = item_func
		self.step = step

	def __len__(self):
		return len(self.table)
	
	def __getitem__(self, key):
		if isinstance(key, slice):
			(start, end, step) = key.indices(len(self.table))
			if step < 0:
				raise TypeError("Negative slice step not allowed in _SeqView")
			if start >= end:
				return []
			start_key = self.table.key_at_index(start)
			if end < len(self.table):
				end_key = self.table.key_at_index(end)
			else:
				end_key = None
			return _SeqView(self.table[start_key:end_key], self.item_func, self.step * step)
		else:
			index = key * self.step	
			key = self.table.key_at_index(index)
			value = self.table[key]
			return self.item_func(key, value)

	def __str__(self):
		return str(list(self))
		
