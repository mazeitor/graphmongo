## GraphMongo: graph framework for mongodb
Minimalist framework to work with directed and non-directed graphs using mongo database and python language programming.

### Features
* Create and remove graph
* Create, update and remove vertex and edges
* Filter vertex and edges using mongodb query language
* Fetch vertex and edges
* Use paging, sort and selects
* Get source and target neighbours
* Deal with multiple graph at the same time
* Use pipeline methods
* Reader/Writer with graphML format

#### Measures and metrics
* Vertex and edges count (number of vertices)
* Vertex degree, vertex in and out degree (number of edges for each vertex, number of in/out-edges for each vertex)
* Graph distances (lentgh and path of the shortest path between vertices for weighted and unweighted)
  - Dijsktra
  - Breadth-first search

### Data model
graph -> nodes, edges  
node -> id, label, weight, data  
edge -> id, label, weight, head, tail, type, data  
data -> json

### Installation
+ Dependencies: [python 2.7](https://wiki.python.org/moin/BeginnersGuide/Download), [pymongo](https://docs.mongodb.com/getting-started/python/client/) and [mongodb](https://docs.mongodb.com/manual/installation/?jmp=footer)
+ Download and use graphmongo.py

### Tutorial
**Create graph**
```python
graph = GraphMongo('localhost', 27018, "directed_graph_01", "directed")
```
**Create nodes**
```python
node1 = graph.AddNode(label=1)
node2 = graph.AddNode(label=2)

```
**Create edge**
```python
edge12 = graph.AddEdge(node_a=node1,node_b=node2)
```
**Get nodes, filter by label and get nodes with format list of ObjectId's**
```python
node1 = graph.GetNodes(label=1)
```
**Get neighbours, filter by nodes with format list of ObjectId's**
```python
relatednodes = graph.GetNeighbours(nodes=node1)
```
**Fetch nodes with format list of ObjectId's and as a return a list of nodes**
```python
fetchednode1 = graph.Fetch(elems=node1)
fetchednodes = graph.Fetch(elems=relatednodes)
```

#### Pipeline methods
Graphmongo allow work with pipeline methods like GetNodes, GetNodes, GetNeighbours and Fetch to grasp nodes and edges from database. One of the input are the nodes or edges collection which could be treat as a parameter of the function or pipe from previous call. This feature give us more flexibility, reuse results and maintain the different stages between calls.   

**Get all nodes and fetch them**
```python
nodelist = graph.GetNodes()   
fetchednodelist = graph.Fetch(nodelist)  
###using pipeline
fetchednodelist = graph.GetNodes().Fetch()
```
**Get neighbour of nodes with label 6 (as a default the neighbour nodes are the tail ones)**
```python
nodes = graph.GetNodes(label=6).GetNeighbours()
```

**Get and fetch nodes related with the edges with label 6**
```python
edges = graph.GetEdges(label=6).GetNeighbours().Fetch()
```

Using **disjunction** option to get desired related nodes. GetNeighbour method grasp all related nodes given as an input a collection of nodes. Sometimes, we would like other kind of answers like, related nodes are not linked with the input nodes (for instance a link between input nodes) or nodes in previous queries. We can achieve that with [Set](https://docs.python.org/2/library/sets.html) operators but when the pipe feature is used we lose this possibility. To solve that problem, the GraphMongo framework has an option to allow works with it. This option is called "disjunction" which is a list type that can takes as an option values "nodes" and "accumulated". The "nodes" value remove the input nodes from the output and the "accumulated" value removes the previous grasped nodes from the output. 

```python
###get related nodes of related nodes of nodes with label 6
nodes = graph.GetNodes(label=6)
nodes = nodes.GetNeighbours()-nodes

###get related nodes of related nodes of nodes with label 6 minus related nodes of nodes with label 6
nodes = graph.GetNodes(label=6).GetNeighbours(disjunction=["nodes"])
```

### Examples
#### Create a basic directed graph
```python
graph = GraphMongo('localhost', 27018, "directed_graph_01")

##create nodes
node6 = graph.AddNode(weight=6)
node5 = graph.AddNode(weight=5)
node3 = graph.AddNode(weight=3)
node1 = graph.AddNode(weight=1)
node2 = graph.AddNode(weight=2)
node4 = graph.AddNode(weight=4)
node9 = graph.AddNode(weight=9)

##create edges
edge65 = graph.AddEdge(node_a=node6,node_b=node2, weight=9)
edge61 = graph.AddEdge(node_a=node6,node_b=node1, weight=14)
edge63 = graph.AddEdge(node_a=node6,node_b=node3, weight=2)
edge13 = graph.AddEdge(node_a=node1,node_b=node3, weight=9)
edge12 = graph.AddEdge(node_a=node1,node_b=node2, weight=7)
edge23 = graph.AddEdge(node_a=node2,node_b=node3, weight=10)
edge24 = graph.AddEdge(node_a=node2,node_b=node2, weight=15)
edge34 = graph.AddEdge(node_a=node3,node_b=node4, weight=11)
edge45 = graph.AddEdge(node_a=node4,node_b=node5, weight=6)

##get all nodes
nodes = graph.GetNodes()
##get filtered nodes
nodes = graph.GetNodes(query={"weight":{"$in":[5,6]}})
##get neighbour nodes given a list of nodes
relatednodes = graph.GetNeighbours(nodes=nodes)
##fetch nodes
fetched = graph.Fetch(elems=nodes)
```

#### Using shortest path methods
GraphMongo framework has implemented the feature to get the shortest path between nodes using weighted and unweighted graph. This algorithms are Dijkstra and Breadth-first search. For using the desired algorithm we only have to put as a parameter the function, for instance, Dijkstra or BreadthFirstSearch. As an ouput of the GraphDistance function it is provided a dictionary, which as a key has the id's of the sources nodes and each value is an object with the attributes "distance" and "from". "distance" is a dictionary with the distance between the source and each target node and "from" is another dictionary with the parent of the node.
```python
###get the nodes with weight equal 6
source = graph.GetNodes(weight=6)
###get the nodes with weight equal 4
target = graph.GetNodes(weight=4)
###get shortestpath between source and targets nodes using dijkstra algorithm for weighted graph
gdDijkstra = graph.GraphDistance(sources=source,targets=target,algorithm=graph.Dijkstra)
###get shortestpath between source and targets nodes using breadth-first search algorithm for unweighted graph
gdBFS = graph.GraphDistance(sources=source,targets=target,algorithm=graph.BreadthFirstSearch)
```

#### Import/Export
GraphMongo allow you to import and export using one of the most popular format for graph how is graphML.

```python
##Read a graph from a file graphML
###create new graph db instance
graph = GraphMongo(address='localhost', port=27018, dbname="graphml")
###import from file
doc = graph.Reader(path="graphml.xml",algorithm=graph.GraphMLR)

##Write a graph instance to graphml file format
doc = graph.Writer(path="graphml.xml",algorithm=graph.GraphMLW)
```

#### TODO
* Implement different metrics and measures like 'connectivity', 'centrality', 'reciprocity and transitivity' and 'homophily, assortative mixing and similarity'  
* Implement use cases like link prediction  

