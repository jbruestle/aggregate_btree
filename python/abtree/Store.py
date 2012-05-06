
import abtree_c
import json
import Table
import threading
import weakref
import time

class Store:
	def __init__(self, 
		name, 
		create = True, 
		serialize_func = json.dumps,
		deserialize_func = json.loads,
		max_write_cache = 10000, 
		max_read_cache = 20000,
		sync_delay = 1):
		self.policy = {
			'serialize' : serialize_func,
			'deserialize' : deserialize_func
		}
		self.store = abtree_c.Store(name, create, max_write_cache, max_read_cache, self.policy)
		self.tables = {}
		self.sync_delay = sync_delay
		self.last_sync = time.time()
		self.waiting_sync = None
		self.lock = threading.RLock()

	def new_table(self, aggregate_func = lambda a,b: None, empty_total = None, cmp_func = cmp):
		new_policy = {
			'serialize' : self.policy['serialize'],
			'deserialize' : self.policy['deserialize'],
			'cmp' : cmp_func,
			'aggregate' : aggregate_func
		}
		with self.lock:
			return Table._Table(abtree_c.DiskTree(self.store, new_policy), self, cmp_func, empty_total, None, None)
	
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
		with self.lock:
			t = Table._Table(self.store.attach(name, new_policy), self, cmp_func, empty_total, None, None)
		self.tables[name] = t
		return t

	def _timer_run(self):
		with self.lock:
			self.sync()
			self.last_sync = time.time()
			self.waiting_sync = None

	def mark(self):
		with self.lock:
			self.store.mark()
			if self.waiting_sync == None:
				when = self.last_sync + self.sync_delay
				delay = max(when - time.time(), 0)
				self.waiting_sync = threading.Timer(delay, self._timer_run)
				self.waiting_sync.start()
			
	def revert(self):
		with self.lock:
			self.store.revert()
			for name in self.tables.values():
				self.table.load()

	def sync(self):
		with self.lock:
			self.store.sync()

	def __getitem__(self, key):
		return self.tables[key]


