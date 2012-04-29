
import abtree_c
import json
import Tree

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

	def new_tree(self, aggregate_func = lambda a,b: None, cmp_func = cmp):
		new_policy = {
			'serialize' : self.policy['serialize'],
			'deserialize' : self.policy['deserialize'],
			'cmp' : cmp_func,
			'aggregate' : aggregate_func
		}
		return Tree.Tree(abtree_c.Tree(self.store, new_policy))
	
	def load(self, 
		name, 
		aggregate_func = lambda a,b: None, 
		cmp_func = cmp):
		new_policy = {
			'serialize' : self.policy['serialize'],
			'deserialize' : self.policy['deserialize'],
			'cmp' : cmp_func,
			'aggregate' : aggregate_func
		}
		return Tree.Tree(self.store.load(name, new_policy))

	def save(self, name, tree):
		self.store.save(name, tree.inner)

	def sync(self):
		self.store.sync()


