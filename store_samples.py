from graphmongo import GraphMongo
import random
import timeit

bases = ['A','C','T','G','N']

def generate():
	gen = ""
	for i in range(1000000):
		value = random.randint(0,4);		
		gen = gen+"A" #str(bases[value])
	return gen;

##loading db
graph = GraphMongo(address='localhost', port=27018, dbname='samples', type='non-directed')
#graph.ClearGraph(metadata=True)

numnodes = 1000

gen = generate()

##createing nodes
start_time = timeit.default_timer()
for i in xrange(0,numnodes):
	weight = generate()
	node = graph.AddNode(weight=weight)
stop_time = timeit.default_timer()
elapsed_time = stop_time - start_time
print('nodes time: ', elapsed_time)

start_time = timeit.default_timer()
graph.GetNodes().Fetch()
stop_time = timeit.default_timer()
elapsed_time = stop_time - start_time
print('time: ', elapsed_time)
