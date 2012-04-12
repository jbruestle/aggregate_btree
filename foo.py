
import btree
import btree_c
import msgpack

store = btree_c.Store("/tmp/wtf", True)
tree = btree.Tree(store, "root", lambda a,b: a + b)
tree['hello'] = 3;
tree['world'] = 5;:
tree2 = tree  # Shallow copy
tree3 = tree.copy() # COW copy
tree['foo'] = 6
tree['bar'] = 7

print "Tree"
for (k, v) in tree:
	print k, ":", v 

print "Tree2"
for (k, v) in tree2:
	print k, ":", v 

print "Tree3"
for (k, v) in tree3:
	print k, ":", v 

