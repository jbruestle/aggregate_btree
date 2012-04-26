
import stat_tree 

tree = stat_tree.StatsTree('/tmp/thetree')
tree.write(0.0, ['tag1'], {'m1':500, 'm2':200})
tree.write(0.0, ['tag2'], {'m1':400, 'm3':300})
tree.write(1.0, ['tag1'], {'m1':300, 'm2':200})
tree.write(1.0, ['tag2'], {'m1':200, 'm3':300})
tree.write(2.0, ['tag1'], {'m1':100, 'm2':200})
tree.write(2.0, ['tag2'], {'m1':100, 'm3':200})
tree.write(3.0, ['tag1'], {'m1':200, 'm2':500})
tree.write(3.0, ['tag2'], {'m1':300, 'm3':200})
tree.write(4.0, ['tag1'], {'m1':400, 'm2':700})
tree.write(4.0, ['tag2'], {'m1':500, 'm3':300})
tree.write(5.0, ['tag1'], {'m1':600, 'm2':200})
tree.write(5.0, ['tag2'], {'m1':700, 'm3':100})

print "Getting tags" 
print tree.tags()
print "all measures:", tree.measures()
print "tag1 measures:", tree.measures('tag1')
print "tag2 measures:", tree.measures('tag2')
print "tag3 measures:", tree.measures('tag3')
print "tag1 info", tree.metrics('tag1', ['m1:cnt', 'm1:avg'], 0.0, 2.0, 5)
