
import abtree_c
import json
import Table

class Store:
	def __init__(self, 
		name, 
		create = True, 
		serialize_func = json.dumps,
		deserialize_func = json.loads,
		max_write_cache = 10000, 
		max_read_cache = 20000):
		self.policy = {
			'serialize' : serialize_func,
			'deserialize' : deserialize_func
		}
		self.store = abtree_c.Store(name, create, max_write_cache, max_read_cache, self.policy)
		self.tables = {}

	def new_table(self, aggregate_func = lambda a,b: None, empty_total = None, cmp_func = cmp):
		new_policy = {
			'serialize' : self.policy['serialize'],
			'deserialize' : self.policy['deserialize'],
			'cmp' : cmp_func,
			'aggregate' : aggregate_func
		}
		return Table._Table(abtree_c.DiskTree(self.store, new_policy), cmp_func, empty_total, None, None)
	
	def attach(self, 
		name, 
		aggregate_func = lambda a,b: None, 
		empty_total = None,
		cmp_func = cmp):
		new_policy = {
			'serialize' : self.policy['serialize'],
			'deserialize' : self.policy['deserialize'],
			'cmp' : cmp_func,
			'aggregate' : aggregate_func
		}
		t = Table._Table(self.store.attach(name, new_policy), cmp_func, empty_total, None, None)
		self.tables[name] = t
		return t

	def mark(self):
		self.store.mark()

	def revert(self):
		self.store.revert()
		for name in self.tables.values():
			self.table.load()

	def sync(self):
		self.store.sync()

	def __getitem__(self, key):
		return self.tables[key]


