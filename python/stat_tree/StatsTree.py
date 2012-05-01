
import abtree

def StatExpand(num):
	return [1, num, num, num, num*num];

def StatAggregate(s1, s2):
	if s1 == None:
		return s2
	if s2 == None:
		return s1
	if isinstance(s1, (int, long, float)):
		s1 = StatExpand(s1)
	if isinstance(s2, (int, long, float)):
		s2 = StatExpand(s2)
	return [s1[0] + s2[0], # Add counts
	        s1[1] + s2[1], # Add total
		max(s1[2], s2[2]), # Get max
		min(s1[3], s2[3]), # Get min
		s1[4] + s2[4]] # Add x^2

#TODO: Make sure this all works with sorted dicts
def StatsAggregate(s1, s2):
	out_dict = {}
	all_keys = set(s1.keys()).union(set(s2.keys()))
	for k in all_keys:
		out_dict[k] = StatAggregate(s1.get(k, None), s2.get(k, None))
	return out_dict

def StatByName(s, name):
	[measure, mtype] = name.split(':')
	row = s.get(measure, [0,0,0,0,0])
	if isinstance(row, (int, long, float)):
		row = StatExpand(row)
	if (mtype == 'cnt'):
		return row[0]
	if (mtype == 'tot'):
		return row[1]
	if (row[0] == 0):
		return None
	if (mtype == 'max'):
		return row[2]
	if (mtype == 'min'):
		return row[3]
	avg = row[1] / row[0]
	if (mtype == 'avg'):
		return avg
	avgsq = row[4] / row[0]
	var = avgsq - avg*avg
	if (mtype == 'var'):
		return var
	if (mtype == 'std'):
		return sqrt(var)
	return None

def DebugLess(a,b):
	r = cmp(a,b)
	print "cmp(",a,",",b,")=", r
	return r

class StatsTree:
	def __init__(self, path):
		self.store = abtree.Store(path, True)
		self.tree = self.store.load("stats", StatsAggregate, {})
		self.counter = 0

	def sync():
		self.store.save("stats", self.tree)
		self.store.sync()

	def write(self, time, tags, measures):
		self.tree[[None, time, self.counter]] = measures
		for tag in tags:
			self.tree[[tag, time, self.counter]] = measures
		self.counter += 1

	def tags(self):
		all_tags = []
		pair = self.tree.lower_bound([None, '', 0])
		while pair != None:
			print "Adding tag", pair[0]
			all_tags += [pair[0]]
			pair = self.tree.lower_bound([pair[0], '', 0])
	
	def measures(self, tag = None):
		all_pairs = self.tree[[tag, None, 0] : [tag, '', 0]].total()
		return all_pairs.keys()
	
	def metrics(self, tag, measures, start, interval, count):
		output = []
		for i in range(0, count):
			allstats = self.tree[[tag, start, 0] : [tag, start + interval, -1]].total()
			row = []
			for m in measures:
				row += [StatByName(allstats, m)]
			output += [row]
			start += interval
		return output
		

