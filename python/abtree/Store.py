
import abtree_c
import json
import Table

class Store:
	def __init__(self, 
		name, 
		create = True, 
		serialize_func = json.dumps,
		deserialize_func = json.loads,
		max_write_cache = 1000, 
		max_read_cache = 10000):
		self.policy = {
			'serialize' : serialize_func,
			'deserialize' : deserialize_func
		}
		self.store = abtree_c.Store(name, create, max_write_cache, max_read_cache, self.policy)

	def new_table(self, aggregate_func = lambda a,b: None, empty_total = None, cmp_func = cmp):
		new_policy = {
			'serialize' : self.policy['serialize'],
			'deserialize' : self.policy['deserialize'],
			'cmp' : cmp_func,
			'aggregate' : aggregate_func
		}
		return Table._Table(abtree_c.DiskTree(self.store, new_policy), cmp_func, empty_total, None, None)
	
	def load(self, 
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
		return Table._Table(self.store.load(name, new_policy), cmp_func, empty_total, None, None)

	def save(self, name, table):
		self.store.save(name, table.inner)

	def sync(self):
		self.store.sync()


