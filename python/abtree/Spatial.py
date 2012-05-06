
import collections
from itertools import imap
from abtree_c import Hilbert
from abtree import Table

class Spatial(collections.MutableMapping):
	def __init__(self, dim, store = None, name = None):
		self.dim = dim
		self.store = store
		self.name = name
		if (store != None):
			# Make tables on disk
			self.forward = store.attach(name + ":forward")
			self.by_location = store.attach(name + ":by_location", Spatial.agg_region, None, Spatial.cmp_location)
		else:
			# In memory only, used dict and table
			self.forward = {}
			self.by_location = Table(Spatial.agg_region, None, Spatial.cmp_location)

	def __contains__(self, key):
		return key in self.forward

	def __len__(self):
		return len(self.forward)

	def __getitem__(self, key):
		return self.forward[key]

	def __setitem__(self, key, value):
		# Verify dimensionality of point
		if (len(value) != self.dim):
			raise ValueError("Tried to insert point of wrong dimensionality")
		# If there is and existing point at this key...
		if (key in self.forward):
			# Get it's location, and remove from location table
			old_val = self.forward[key]
			del self.by_location[(old_val, key)]
		# Add new point to dict and location table
		self.forward[key] = value
		self.by_location[(value, key)] = (value, value)

	def __delitem__(self, key):
		# Get location of key (or raise KeyError)
		old_val = self.forward[key] 
		# Delete from map and location table
		del self.forward[key]
		del self.by_location[(old_val, key)]

	def __iter__(self):
		return iter(self.forward)

	def within(self, p1, p2):
		# Verify proper dimensionality
		if len(p1) != self.dim or len(p2) != self.dim:
			raise ValueError("Invalid dimensionality in 'within'")
		# Compute canonical form of bounding box
		min_bounds = map(min, p1, p2)
		max_bounds = map(max, p1, p2)
		box = (min_bounds, max_bounds)
		# Get an iterator that will walk over all elements in
		# by_location that intersect wit the query box
		it = self.by_location.find(lambda v: Spatial.box_intersect(v, box)) 
		# Covert the keys of by_location (loc, key) to straight keys
		return imap(lambda k: k[1], it)

	# Computes the bounding box that holds both inputs bounding boxs
	@staticmethod
	def agg_region(box1, box2):
		min_bounds = map(min, box1[0], box2[0])
		max_bounds = map(max, box1[1], box2[1])
		return (min_bounds, max_bounds)

	# Determines if two bounding boxes intersect
	@staticmethod
	def box_intersect(box1, box2):
		for i in range(0, len(box1)):
			if (box1[0][i] > box2[1][i]):  # If the min of box1 > the max of box 2
				return False # They don't intersect
			if (box2[0][i] > box1[1][i]):  # If the min of box2 > the max of box 1
				return False # They don't intersect
		return True  # Otherwise, the do intersect

	# Compares by location and then key
	@staticmethod
	def cmp_location(k1, k2):
		# Try to compare by location
		r = Hilbert.cmp(k1[0], k2[0])
		# If identical, fall back to key
		if r == 0:
			r = cmp(k1[1], k2[1])
		return r

