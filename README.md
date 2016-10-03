## GraphMongo: graph framework for mongodb
Minimalist framework to work with directed graphs using mongo database and python language programming.

### Features
* Create and remove graph
* Create, update and remove vertex and edges
* Filter vertex and edges using mongodb query language
* Fetch vertex
* Use paging, sort and selects
* Get head and tail neighbours
* Deal with multiple graph at the same time

#### Measures and metrics
* Vertex and edges count
* Vertex degree

### Data model
graph -> nodes, edges  
node -> id, label, weight, data  
edge -> id, label, weight, head, tail, type, data  
data -> json

### Installation
+ Dependencies: python 2.7, pymongo and mongodb
+ Download and use graphmongo.py

### Tutorial
Create graph
```python
graph = GraphMongo('localhost', 27018, "directed_graph_01")
```
Create nodes
```python
node1 = graph.AddNode(label=1)
node2 = graph.AddNode(label=2)

```
Create edge
```python
edge12 = graph.AddEdge(head=node1,tail=node2)
```
Get nodes, filter by label and get nodes with format list of ObjectId's
```python
node1 = graph.GetNodes(label=1)
```
Get neighbours, filter by nodes with format list of ObjectId's
```python
relatednodes = graph.GetNeighbours(nodes=node1)
```
Fetch nodes with format list of ObjectId's and as a return a list of nodes
```python
fetchednode1 = graph.Fetch(elems=node1)
fetchednodes = graph.Fetch(elems=relatednodes)
```

####Pipeline methods
Graphmongo allow work with pipeline methods like GetNodes, GetNeighbours and Fetch to grasp nodes from database. One of the input are the nodes collection which could be treat as a parameter of the functiona or pipe from previous call. This feature give us more flexivility, reuse results and maintaing the different stage between calls.   

Get all nodes and fetch them
```python
nodelist = graph.GetNodes()   
fetchednodelist = graph.Fetch(nodelist)  
###using pipeline
fetchednodelist = graph.GetNodes().Fetch()
```
Get neighbour of nodes with label 6
```python
nodes = graph.GetNodes(label=6).GetNeighbours()
```
Using *disjunction* option to get desired related nodes. GetNeighbour method grasp all related nodes given as an input a collection of nodes. Sometimes, we would like other kind of answers like, related nodes are not linked with the input nodes (for instance a link between input nodes) or nodes in previous queries. We can achieve that with [Set](https://docs.python.org/2/library/sets.html) operators but When we are using the pipe feature we lose this possibility so we have added this options to allow configure our desired query. Disjunction is a list that can take "nodes" or "accumulated" option. 
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
edge65 = graph.AddEdge(head=node6,tail=node2, weight=9)
edge61 = graph.AddEdge(head=node6,tail=node1, weight=14)
edge63 = graph.AddEdge(head=node6,tail=node3, weight=2)
edge13 = graph.AddEdge(head=node1,tail=node3, weight=9)
edge12 = graph.AddEdge(head=node1,tail=node2, weight=7)
edge23 = graph.AddEdge(head=node2,tail=node3, weight=10)
edge24 = graph.AddEdge(head=node2,tail=node2, weight=15)
edge34 = graph.AddEdge(head=node3,tail=node4, weight=11)
edge45 = graph.AddEdge(head=node4,tail=node5, weight=6)

##get all nodes
nodes = graph.GetNodes()
##get filtered nodes
nodes = graph.GetNodes(query={"weight":{"$in":[5,6]}})
##get neighbour nodes given a list of nodes
relatednodes = graph.GetNeighbours(nodes=nodes)
##fetch nodes
fetched = graph.Fetch(elems=nodes)
```

#### Dijkstra shortest path algorithm
```python

```

#### TODO
* Implement different metrics and measures like 'distances', 'connectivity', 'centrality', 'reciprocity and transitivity' and 'homophily, assortative mixing and similarity'  
* Implement use cases like Dijkstra or link prediction  
