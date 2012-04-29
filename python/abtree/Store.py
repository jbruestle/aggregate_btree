
import abtree_c

class Store:
	def __init__(self, name, create = True, max_write_cache = 1000, max_read_cache = 10000):
		self.store = abtree_c.Store(name, create, max_write_cache, max_read_cache)
