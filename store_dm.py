from graphmongo import GraphMongo
import random
import timeit

##loading db
graph = GraphMongo(address='localhost', port=27018, dbname='dm', type='non-directed')
graph.ClearGraph(metadata=True)

numnodes = 1000

nodelist = []

##createing nodes
start_time = timeit.default_timer()
for i in xrange(0,numnodes): 
	node = graph.AddNode(weight=i)
        nodelist.append(node)
stop_time = timeit.default_timer()
elapsed_time = stop_time - start_time
print('nodes time: ', elapsed_time)

##creating edges
start_time = timeit.default_timer()
for i in xrange(0,numnodes):
	for j in xrange(0,numnodes):
		value = 1000 ##random.randint(0,1000);	
		if j == 1:
			value = 1	

		graph.AddEdge(node_a=nodelist[i],node_b=nodelist[j], weight=value)

stop_time = timeit.default_timer()
elapsed_time = stop_time - start_time
print('edges time: ', elapsed_time)

start_time = timeit.default_timer()
print "Get nodes with closesness less than 20 SNP"
edges = graph.GetEdges(query={"weight": { '$gte' :0 , '$lte' : 20 }})
print "edges", len(edges)
edges.Fetch(type="edge")
#relatednodes = graph.GetNeighbours(edges = edges)
#print "nodes ",len(relatednodes)
#relatednodes.Fetch()
stop_time = timeit.default_timer()
elapsed_time = stop_time - start_time
print('time: ', elapsed_time)
