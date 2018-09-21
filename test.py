from graphmongo import GraphMongo
import random
import timeit

##loading db
graph = GraphMongo(address='localhost', port=27018, dbname='directed_graph', type='non-directed')
graph.ClearGraph(metadata=True)

numnodes = 10

##createing nodes
start_time = timeit.default_timer()
for i in xrange(1,numnodes): 
	graph.AddNode(weight=i)
stop_time = timeit.default_timer()
elapsed_time = stop_time - start_time
print('nodes time: ', elapsed_time)

##creating edges
elapsed_time = 0
for i in xrange(1,numnodes):
        node1 = graph.GetNodes(weight=i).Fetch()[0]
	for j in xrange(1,numnodes):
		node2 = graph.GetNodes(weight=j).Fetch()[0]
		value = random.randint(0,1000);	
		if j == 1:
			value = 1	
		start_time = timeit.default_timer()

		edge = graph.AddEdge(node_a=node1,node_b=node2, weight=value)
		stop_time = timeit.default_timer()
		elapsed_time = elapsed_time + (stop_time - start_time)
print('edges time: ', elapsed_time)

##queries
print "Get all nodes"
nodes = graph.GetNodes()
#print nodes.Fetch()

print "Get all edges"
edges = graph.GetEdges()
#print edges.Fetch(type="edge")

print "Get node 5"
node = graph.GetNodes(weight=5)
print node.Fetch()

print "Get nodes with closesness less than 20 SNP"
edges = graph.GetEdges(query={"weight": { '$gte' :0 , '$lte' : 20 }})
print "edges"
print edges.Fetch(type="edge")
relatednodes = graph.GetNeighbours(edges = edges)
print "nodes"
print relatednodes.Fetch()

print "Get nodes related with node 5"
nodes5 = node.GetNeighbours()
#print nodes5.Fetch()

print "Get nodes related with 5 with less than 20 SNP"
nodes5 = node.GetNeighbours(query={"weight": { '$gte' :0 , '$lte' : 20 } })
print nodes5.Fetch()
