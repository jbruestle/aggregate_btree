
import abtree

store = abtree.Store("/tmp/wtf", True)
tree = store.attach("test")

tree['hello'] = 3
tree['world'] = 5
tree2 = tree  # Shallow copy
tree3 = tree.copy() # COW copy
tree['foo'] = 6
tree['bar'] = 7

print "Tree"
for (k, v) in tree.items():
	print k, ":", v 

print "Tree2"
for (k, v) in tree2.items():
	print k, ":", v 

print "Tree3"
for (k, v) in tree3.items():
	print k, ":", v 

store.mark()
store.sync()

tree = None
tree2 = None
tree3 = None
store = None
store = abtree.Store("/tmp/wtf", True)
tree = store.attach("test")

print "Reloaded"
for (k, v) in tree.items():
	print k, ":", v 
	
