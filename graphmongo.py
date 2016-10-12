import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import random
import math
import time
import argparse
import heapq
import pdb
import Queue


'''
Created on 01 July 2016
@author: oriol mazariegos
'''
class Utils():
	'''
	Utils class for check, validations and wrappers
	'''
	@classmethod
	def wrapElems(self,elem):
		'''
		@brief: change elem type to list of ObjectId's
		@param elem: object with elements
		@return: list of ObjectId's 
		'''
               	if isinstance(elem,GraphMongo):
                       	elem=list(set(elem))
               	elif isinstance(elem,set):
                        elem=list(elem)
		elif not isinstance(elem,list) and elem is not None:
                       	elem=[elem]

      		return elem


class GraphMongo(MongoClient, set):
	'''
	Graph class for mongodb database
	'''
	
	address = "localhost"  	###databse ip address
	port = 27017           	###databae listen port

	_ddbb = "graph"        	###database name
	_node = "node"         	###collection name for nodes
	_edge = "edge"         	###collection name for esges

	_accumulated = set([]) 	###adding nodes from previous queries
	_pipetype = "node" 	###pipe type for pipe feature of some functions


	def __init__(self, address="localhost", port=27017, dbname="graph", results=set([]), connection=True):
		''' 
        	@brief: init a connection with mongo ddbb
        	@param address: ip address where the database is located 
        	@param port: port where the database is listening
		@param dbname: name for the graph database
                @param results: list of ObjectId to initialize the instance with previous queries 
		'''
		self.SetParameters(address,port,dbname,results)
		if connection == True:
			super(GraphMongo,self).__init__(self.address,self.port)
		

        def SetParameters(self, address=None, port=None, dbname=None, results=None):
                '''
                @brief: set parameters like address and port of the mongo instance, name of the database and results of previous query
                @param address: ip address where the database is located
                @param port: port where the database is listening
                @param dbname: name for the graph database
                @param results: list of ObjectId to initialize the instance with previous queries
                '''
                if address is not None:
                        self.address = address
                if port is not None:
                        self.port = port
                if address is not None or port is not None:
                        MongoClient.__init__(self,self.address,self.port)
                if dbname is not None:
                        self._ddbb = dbname
                if results is not None:
                        self.clear()
                        self.update(results)


	def _CopyObject(self):
		'''
		@brief: copy full object
		@return: GraphMongo element
		'''
		##don't need to open a new connection because was already opened
		graph = GraphMongo(address=self.address, port=self.port, dbname=self._ddbb, results=set(self), connection=False) 
		graph._accumulated = self._accumulated	
		graph._pipetype = self._pipetype

		return graph

	
	def _Reset(self):
		'''
		@brief: remove current and accumulated results come from previous queries
		@return: graphmongo object
		'''
		##remove current values
		self.clear()
		##remove accumulated values
		self._accumulated.clear()
	
		aux = self._CopyObject() ###pipeline method
		return aux

        
	def ClearGraph(self, nodes=True, edges=True):
                '''
                @brief: remove all nodes and edges of the ddbb taking account the params
                @param nodes: option to delete all nodes
                @param edges: option to delete all edges
                '''
                if nodes:
                        self[self._ddbb][self._node].remove()
                if edges:
                        self[self._ddbb][self._edge].remove()


	def AddNode(self,node=None, label=None, weight=None, data=None):
		'''
		@brief: create a new node in the ddbb
		@param node: dictionary with a node definition, params: _id, label and data
		@param label: type name of the node
		@param weight: weight of the node
		@param data: extra information
		@return: node created in the ddbb otherwise an error is returned as dictionary with status=ko  
		'''	
		try:	
			if node is None:
				node = {}
				node["_id"]=None

			if node["_id"] is None:
				node["_id"]=ObjectId()

			if label is not None:
				node["label"]=label

			if weight is not None:
				node["weight"]=weight
                       
			if data is not None:
                                node["data"]=data


			self[self._ddbb][self._node].insert(node)
			return node
		except:
			return {"status":"ko"}

	def AddEdge(self, edge=None, label=None, weight=None, head=None, tail=None, data=None, type="directed"):
		'''
		@brief: create a new edge in the ddbb
		@param edge: dictionary with an edge definition, params: _id, label, head (node 1), tail(node 2) and data
		@param label: relation name of the edge
                @param weight: weight of the edge
		@param data: extra information
		@param type: type of the edge, "directed" or "simple"
		@return: edge created in the ddbb, otherwise an error is returned as dictionary with status ko
        	'''
		try:
			if edge is None:
				edge = {}
				edge["_id"]=None
				
			if edge["_id"] is None:
				edge["_id"]=ObjectId()

			if label is not None:
				edge["label"]=label

			if data is not None:
				edge["data"]=data
			
                        if weight is not None:
                                edge["weight"]=weight
		
			if type is not None and type not in ["directed","simple"]:
				return {"status":"ko"}
			else:
				edge["type"]=type

			if head is not None and tail is not None:
				if type == "simple":
					pass ##TODO simple and bidirectional graph
				else:
					headref = {"_id" : head["_id"]}
                        		tailref = {"_id" : tail["_id"]}
			
				edge["head"]=headref
                                edge["tail"]=tailref
	
			self[self._ddbb][self._edge].insert(edge)
			return edge
		except:
			return {"status":"ko"}

	def UpdateNode(self, node=None):
                '''
                @brief: update desired node, _id and label field cannot be updated, remove node and create another one
                @param node: dictionary with a node definition, params: _id, label and data
                @return: node updated in the ddbb otherwise an error is returned as dictionary with status=ko
                '''

		if node is None:
			return {"status":"ko"}
		else:
			res = self[self._ddbb][self._node].save(node)
			return node


	def UpdateEdge(self, edge=None):
                '''
                @brief: update desired edge, _id, head and tail cannot be updated, remove edge and create another one
                @param edge: dictionary with an edge definition, params: _id, label, head (node 1), tail(node 2) and data
                @return: edge updated in the ddbb otherwise an error is returned as dictionary with status=ko
                '''
                if edge is None:
                        return {"status":"ko"}
                else:
			res = self[self._ddbb][self._edge].save(edge)
                        return edge
	

	def RemoveNode(self, node=None):
		'''
		@brief: remove an specific node from the ddbb
		@param node: dictionary with a node definition, params: _id, label and data
		@return: dictionary with status 'ok' or 'ko' 
		'''

                if node is None or "_id" not in node.keys():
                        return {"status":"ko"}

		try:
			self[self._ddbb][self._node].remove(node)

			###removes edges in and out
			ref = {"_id" : node["_id"], "label" : node["label"]}
			head = {"head" : ref}
			tail = {"tail" : ref}
		
			self[self._ddbb][self._edge].remove(head)	
			self[self._ddbb][self._edge].remove(tail)	
			return {"status":"ok"}
		except:
			return {"status":"ko"}

	def RemoveEdge(self, edge=None):
		'''
		@brief: remove an specific edge head the ddbb
		@param edge: dictionary with an edge definition, params: _id, label, head (node 1), tail(node 2) and data
		@return: dictionary with status 'ok' or 'ko'
		'''
		
                if edge is None or "_id" not in edge.keys():
                        return {"status":"ko"}

		try:
			##remove edge
			self[self._ddbb][self._edge].remove(edge)
			return {"status":"ok"}
		except:
			return {"status":"ko"}


	def __paging(self, elems, page, per_page):
                '''
                @brief: apply paging given a cursor collection
                @param elems: cursos collection of documents 
                @param page: start index page
		@param per_page: the number of entries shown per page
		@return: cursor collection
		'''
		try:
			if page > 0:
				page = (page-1)*per_page+per_page
			else:
				page = 0
			elems = elems.skip(page).limit(per_page)
			return elems
		except:
			return {"status":"ko"}


    	def __sorting(self, elems, attributes):
                '''
                @brief: apply sorting given a cursor collection
                @param elems: cursos collection of documents
                @param attributes: list of attributes ex: {"attribute_name": ["descending,ascending"], "atrribute_name": ["descending,ascending"]}
                @return: cursor collection
                '''
		try:
        		elems = elems.sort(str(attributes[0][0]),attributes[0][1])
        		return elems
		except:
                        return {"status":"ko"}


        def Fetch(self, elems=None, type="node", projection=None, sort=None, paging=None):
                '''
                @brief: generic fetch function to grasp from ddbb a list of elements
                @param elem: element search head ddbb
                @param type: type element, node or edge
                @param projection: list of attributes you want retrive {"attribute_name":"false|true",...}
                @param sort: list of attributes you want sort the results in mongodb sort format, {"attributename":"ascending|descending",...}
                @param paging: page and per_page, start index page and the number of entries shown per page
                @return: list of fetched nodes, otherwise an error is returned as dictionary with status ko
                '''
		try:
                        query = {}
                        ids = []

			if elems is None: ###pipeline method
				elems = list(set(self))
			else:
				elems = Utils.wrapElems(elems)# and isinstance(elems,set):
				#elems = list(elems)

			#elems = Utils.wrapElems(elems)

                        ids = elems
                        if len(ids) > 0:
                                query["_id"]={}
                                query["_id"]["$in"]=ids

                        if projection is None:
                                elems = self[self._ddbb][type].find(query)
                        else:
                                elems = self[self._ddbb][type].find(query,projection)

                        ###sorting
                        if sort is not None:
                                elems = self.__sort(elems,sort)

                        ###paging
                        if paging is not None:
                                page = paging["page"]
                                per_page = paging["per_page"]
                                elems = self.__paging(elems, page, per_page)

                        return list(elems)
                except:
                        return {"status":"ko"}

	
	def __Get(self, elems=None, type=None, query=None):
		'''
		@brief: generic get function get an element head the ddbb with a parametrized resource, node or edge
		@param elems: list of ObjectId's 
		@param type: type element, node or edge
		@param query: mongodb expression query applyed in the ddbb
		@return: list of nodes, otherwise an error is returned as dictionary with status ko
		'''
		try:	
			###at the moment the query result applied is or by list of id's or the query passed as a parameter
			if query is None:
				query = {}
				
				###if there is no id tail get, then we will get everything
				if elems and len(elems) > 0:
					query["_id"]={}
					query["_id"]["$in"]=elems		

			if type is None:
				return {"status":"ko"}
			else:
				projection = {"_id":1}
				elems = self[self._ddbb][type].find(query,projection)

			ids = [elem["_id"] for elem in elems]
                        return list(set(ids))
		except:
			return {"status":"ko"}


	def GetEdges(self, edges=None, head=None, tail=None, label=None, weight=None, direction=None, query=None):
		'''
                @brief: get list of edges given id's, label or quering
                @param edges: list of edges
		@param label: label of the node or relation
                @param weight: weight of the node or relation
                @param direction: direction of the relation, "head|tail"
                @param query: mongodb expression query applyed in the ddbb
		@return list of edges, otherwise an error is returned as dictionary with status ko
		'''
                try:
                        elems=[]
			if query is None:
                        	query = {}

                        if label is not None:
                                query.update({"label" : label})

                        if weight is not None:
                                query.update({"weight" : weight})	
			
                        if head is not None:
				head = Utils.wrapElems(head)
				query.update({"head._id" : {"$in" : head}})
			if tail is not None:
                                tail = Utils.wrapElems(tail)
				query.update({"tail._id" : {"$in" : tail}})
                        
			elems = set(self.__Get(elems=edges, type="edge", query=query))

                        ##defining pipeline method output
			self._pipetype = "edge"
                        aux = self._CopyObject()
                        aux.SetParameters(results=elems)
                        return aux
                except:
                        return {"status":"ko"}


	def GetNodes(self, label=None, weight=None, direction=None, query=None):
	        '''
                @brief: get list of nodes given label, weight or other kind of query
                @param label: label of the node or relation
                @param weight: weight of the node or relation
                @param query: mongodb expression query applyed in the ddbb
                @return: graphmongo instance with list of nodes, otherwise an error is returned as dictionary with status ko
                '''
		try:
			if label is not None:
				if query is None:
					query = {}
				query.update({"label" : label})

			if weight is not None:
				if query is None:
					query = {}
				query.update({"weight" : weight})
                        
			##reset values as a first endpoint 
			self._Reset() 
			
			elems = set(self.__Get(type="node", query=query))

			##defining pipeline method output
			self._pipetype = "node"
			aux = self._CopyObject() 
			aux.SetParameters(results=elems)
			return aux
		except:
	                return {"status":"ko"}


	def GetNeighbours(self, nodes=None, edges=None, label=None, weight=None, direction=None, query=None, disjunction=None):
                '''
                @brief: get list of related nodes given nodes or related nodes with edges
                @param nodes: list of ObjectId's of nodes
                @param edges: list of ObjectId's of edges
                @param label: label of the node or relation
                @param weight: weight of the node or relation
                @param direction: direction of the relation, "head|tail"
                @param query: mongodb expression query applyed in the ddbb
		@param disjunction: option to delete in the result the previous queries
                @return: graphmongo instance with list of nodes, otherwise an error is returned as dictionary with status ko
                '''
                elems=[]

		

		#param manager for pipeline method
		if nodes is None:
			if set(self) is not None and len(self)>0 and self._pipetype=="node":
				nodes = list(set(self))
              		#elif edges is None:
			#	nodes = list()
		if edges is None:
			if set(self) is not None and len(self)>0 and self._pipetype=="edge":
				edges = list(set(self))
	
		if nodes is None and edges is None:
			nodes = list()	
		if nodes is None and edges is not None:
			nodes = list()
			edges = Utils.wrapElems(edges)
                        elems += self.__GetNodeNeighbours(edges=edges, direction=direction)
		else:
			nodes = Utils.wrapElems(nodes)
			edges = Utils.wrapElems(edges)
			elems = self.__GetNodeNeighbours(nodes=nodes, edges=edges, label=label, weight=weight, query=query, direction=direction)
              

		#if disjunction is None or ("nodes","accumulated") not in disjunction: 
		elems = set(elems) 
	
		if disjunction is None:
			disjunction = list()	
		if "nodes" in disjunction:
			elems = elems - set(nodes)
		if "accumulated" in disjunction:
			elems = elems - set(self._accumulated)

		aux = self._CopyObject() ### pipeline method
		aux.SetParameters(results=elems)
                aux._accumulated = aux._accumulated | set(nodes) 
		
		return aux


        def __GetNodeNeighbours(self, nodes=None, edges=None, label=None, weight=None, query=None, direction=None):
                '''
                @brief: get list of nodes related with the nodes given in the parameter, also label and direction can be specified
                @param nodes: list of ObjectId's of nodes tail or head
		@param edges: list of ObjectId's of edges
                @param label: label of the node or relation
                @param weight: weight of the node or relation
		@param direction: direction of the relation, head or tail
                @param projection: list of attributes you want retrive {"attribute_name":"false|true",...}
                @param sort: list of attributes you want sort the results in mongodb sort format, {"attributename":"ascending|descending",...}
                @param paging: page and per_page, start index page and the number of entries shown per page
                @return: list of nodes, otherwise an error is returned as dictionary with status ko
		'''

		###direction have tail be "head" or "tail" depending if we want tail retrieve head or tail
		if direction is not None and direction not in ["head","tail"]:
			return {"status":"ko"}
		try:
			if direction is None:
				FROM = "head"	
				TO = "tail"
			else:
				FROM = direction
				if FROM == "head":
					TO = "tail"
				elif FROM == "tail":
					TO = "head"

			if label is not None:
				if query is None:
					query = {}
				query.update({"label" : label})

                        if weight is not None:
                                if query is None:
                                        query = {}
                                query.update({"weight" : weight})

			if nodes is None and edges is not None:
				if edges and any(not isinstance(edge,(ObjectId)) for edge in edges):
                                	edges = [edge["_id"] for edge in edges]

                                match = {"$match" : {"_id" : {"$in" : edges}}}
                                project = {"$project" : {"_id" : "${0}._id".format(TO)}}
                                elems = self[self._ddbb][self._edge].aggregate([match,project])
			elif nodes is not None and edges is not None:
                                if nodes and any(not isinstance(node,(ObjectId)) for node in nodes):
	                                nodes = [node["_id"] for node in nodes]
                                if edges and any(not isinstance(edge,(ObjectId)) for edge in edges):
                                        edges = [edge["_id"] for edge in edges]

				if query:	
					query = {"$and":[{"_id" : {"$in" : edges}},{"{0}._id".format(FROM) : {"$in" : nodes}},query]}
                                else:
					query = {"$and":[{"_id" : {"$in" : edges}},{"{0}._id".format(FROM) : {"$in" : nodes}}]}
				match = {"$match" : query}
                                project = {"$project" : {"_id" : "${0}._id".format(TO)}}
                                elems = self[self._ddbb][self._edge].aggregate([match,project])
			else:	
				if nodes and any(not isinstance(node,(ObjectId)) for node in nodes):
                                                nodes = [node["_id"] for node in nodes]

				if query is not None:
					if nodes:
						query = {"$and":[{"{0}._id".format(FROM) : {"$in" : nodes}},query]}
					match = {"$match" : query}
                                        project = {"$project" : {"_id" : "${0}._id".format(TO)}}
                                        elems = self[self._ddbb][self._edge].aggregate([match,project])
				else:
					match = {"$match" : {"{0}._id".format(FROM) : {"$in" : nodes}}}
					project = {"$project" : {"_id" : "${0}._id".format(TO)}}
					elems = self[self._ddbb][self._edge].aggregate([match,project])
			ids = [elem["_id"] for elem in elems]
			return list(set(ids))
		except:
			return {"status":"ko"}


	##################################
	###### MEASURES AND METRICS ######
	##################################
	def VertexCount(self):
		'''
		@brief: gives a count of the number of vertices in the graph
		@return: number of nodes
		'''
		return self[self._ddbb][self._node].count()


	def EdgeCount(self):
		'''
		@brief: gives a count of the number of edges in the graph
		@return: numeber of edges
		'''
		return self[self._ddbb][self._edge].count()

	def VertexDegree(self, nodes=None):
		'''
		@brief: gives the list of vertex degrees for all nodes or nodes passed in the parameter in the graph
		@param nodes: list of nodes
		@return: dictionary with the nodeid and its degree
		'''
		###pipeline method
		elems={}

		##param pipeline manager
                if nodes is None and set(self) is not None and len(self)>0:
                        nodes = list(set(self))

		if nodes is None:
			nodes = list()	
			nodes = self.GetNodes()
		
		if nodes:
			nodes = Utils.wrapElems(nodes)

		for node in nodes:
		        outdegree = self.__GetNodeNeighbours(nodes=[node],direction="tail")
	                indegree = self.__GetNodeNeighbours(nodes=[node],direction="head")
			degree = len(outdegree)+len(indegree)
			elems[node]=degree
		return elems	


	################################
	##### DISTANCES ALGORITHMS #####
	################################
	def GraphDistance(self, sources, targets=None, algorithm=None):
		'''
		@brief: gives the distance from source vertes to target vertex
		@param sources: list of ObjectId's of source nodes
		@param targets: list of ObjectId's of target nodes		
		@param algorithm: function to be called as a parameter. the function have to follow the input/output like __GraphDistance(self, source, target=None) generic function
		@return: dictionary with relation of sources and targets. This relation is a list of intermediate ObjectId nodes and weights
		'''
		
		if targets is None:
			targets = self.GetNodes()

		if sources:
			sources = Utils.wrapElems(sources)			

                if targets:
			targets = Utils.wrapElems(targets)
		
		elems={}
		
		for source in sources:
			elems[source] = {}
			if algorithm is None:
				algorithm = self.BreadthFirstSearch
			distance = algorithm(source=source, targets=targets)
			elems[source] = distance
		return elems


        def AStar(self, source, target=None):
                '''
                @brief: gives the distance and previous node from source vertex to target vertex, complexity=O(log h* (x))
                @param source: ObjectId of source node
                @param target: list of ObjectId of target nodes
                @return: dictionary with relation of source and targets. fist the tag "distance" give the distance from source to target and in the "from" the node where we have arrived to the current node
                '''

                return {"distance":0,"path":set([])}


        def UniformCostSearch(self, source, target=None):
                '''
                @brief: gives the distance and previous node from source vertex to target vertex, complexity=O(b^(1 + C / epsilon))
                @param source: ObjectId of source node
                @param target: list of ObjectId of target nodes
                @return: dictionary with relation of source and targets. fist the tag "distance" give the distance from source to target and in the "from" the node where we have arrived to the current node
                '''
                return {"distance":0,"path":set([])}



	def BreadthFirstSearch(self, source, targets=None):
                '''
                @brief: gives the distance and previous node from source vertex to target vertex for unweighted graphs. complexity=O(|E|+|V|)
                @param source: ObjectId of source node
                @param target: list of ObjectId of target nodes
                @return: dictionary with relation of source and targets. fist the tag "distance" give the distance from source to target and in the "from" the node where we have arrived to the current node
                '''
                ###initialization
                prev = dict()
                dist = dict()
                seen = dict()
                dist[source] = 0

                ###create a vertex set Q
                Q = []
                heapq.heappush(Q,source)

                while Q: ###main loop
                        u = heapq.heappop(Q) ###remove and return best vertex
                        seen[u] = True
                        neighbours = self.GetNeighbours(nodes=[u])

                        for v in set(neighbours):
                                ###check if the node have been seen before
                                if seen.has_key(v) and seen[v] == True: continue

                                ###get distance between nodes
				distance = 1

                                alt = dist[u] + distance
                                dist[v] = alt
                                prev[v] = u
                                heapq.heappush(Q,v)

				###remove from targets, leave condition
				if v in targets:
					targets.remove(v)

                return {"distance":dist,"from":prev}


	def Dijkstra(self, source, targets=None):
                '''
                @brief: gives the distance and previous node from source vertex to target vertex for weighted graph. complexity=O((|E|+|V|) log |V|) = O(|E| log |V|) because we are using priority queue
                @param source: ObjectId of source node
                @param target: list of ObjectId of target nodes
                @return: dictionary with relation of source and targets. fist the tag "distance" give the distance from source to target and in the "from" the node where we have arrived to the current node
                '''
		###initialization
		prev = dict()
		dist = dict()
		seen = dict()
		dist[source] = 0

		###create a vertex set Q
		Q = []	
		heapq.heappush(Q,(source, dist[source]))
		for target in targets:
			if source != target:
				dist[target] = float('inf')	###infinite
				prev[target] = None		###undefined
				seen[target] = False		###unseen
				heapq.heappush(Q,(target, dist[target])) ### add target node to queue
		
		while Q: ###main loop
			item = heapq.heappop(Q) ###remove and return best vertex
			u = item[0]
			seen[u] = True	
			neighbours = self.GetNeighbours(nodes=[u])
					
			for v in set(neighbours):
				###check if the node have been seen before
				if seen.has_key(v) and seen[v] == True:	continue
				
				###check if the distance to new node is in the dist structure
				if not dist.has_key(v):
                                        dist[v] = float('inf')

				###get distance between nodes
				edges = self.GetEdges(head=u,tail=v).Fetch(type="edge")
				distance = float('inf')
				for edge in edges:
					if edge["weight"]<distance:
						distance=edge["weight"]

				alt = dist[u] + distance
				if alt < dist[v]:
					dist[v] = alt
					prev[v] = u
					heapq.heappush(Q,(v, dist[v]))
		
		return {"distance":dist,"from":prev}	


def CreateDirectedGraph():

	##create instance for graphAPI for mongodb
        graph = GraphMongo('localhost', 27018, dbname="graph")
	##remove all previous data, nodes and edges 
        graph.ClearGraph()

	##create nodes
        node6 = graph.AddNode(weight=6)
        node5 = graph.AddNode(weight=5)
        node3 = graph.AddNode(weight=3)
        node1 = graph.AddNode(weight=1)
        node2 = graph.AddNode(weight=2)
        node4 = graph.AddNode(weight=4)
        node9 = graph.AddNode(weight=9)
	
	##create edges
        edge65 = graph.AddEdge(head=node6,tail=node5, weight=9)
        edge56 = graph.AddEdge(head=node5,tail=node6, weight=9)
        
	edge61 = graph.AddEdge(head=node6,tail=node1, weight=14)
        edge16 = graph.AddEdge(head=node1,tail=node6, weight=14)
	
	edge63 = graph.AddEdge(head=node6,tail=node3, weight=2)
        edge36 = graph.AddEdge(head=node3,tail=node6, weight=2)
	
	edge13 = graph.AddEdge(head=node1,tail=node3, weight=9)
        edge31 = graph.AddEdge(head=node3,tail=node1, weight=9)
        
	edge12 = graph.AddEdge(head=node1,tail=node2, weight=7)
        edge21 = graph.AddEdge(head=node2,tail=node1, weight=7)
        
	edge23 = graph.AddEdge(head=node2,tail=node3, weight=10)
        edge32 = graph.AddEdge(head=node3,tail=node2, weight=10)

	edge24 = graph.AddEdge(head=node2,tail=node2, weight=15)
	edge42 = graph.AddEdge(head=node4,tail=node4, weight=15)
	
        edge34 = graph.AddEdge(head=node3,tail=node4, weight=11)
        edge43 = graph.AddEdge(head=node4,tail=node3, weight=11)
        
	edge45 = graph.AddEdge(head=node4,tail=node5, weight=6)
        edge54 = graph.AddEdge(head=node5,tail=node4, weight=6)
      
	graph.close() 

def CreateSimpleGraph():

        ##create instance for graphAPI for mongodb
        graph = GraphMongo('localhost', 27018)
	graph.SetParameters(dbname="graph")
        ##remove all previous data, nodes and edges
        graph.ClearGraph()

        ##create nodes
        node6 = graph.AddNode(weight=6)
        node5 = graph.AddNode(weight=5)
        node3 = graph.AddNode(weight=3)
        node1 = graph.AddNode(weight=1)
        node2 = graph.AddNode(weight=2)
        node4 = graph.AddNode(weight=4)
        node9 = graph.AddNode(weight=9)

        ##create edges
        edge65 = graph.AddEdge(head=node6,tail=node2, weight=9, type="simple")
        edge61 = graph.AddEdge(head=node6,tail=node1, weight=14, type="simple")
        edge63 = graph.AddEdge(head=node6,tail=node3, weight=2, type="simple")
        edge13 = graph.AddEdge(head=node1,tail=node3, weight=9, type="simple")
        edge12 = graph.AddEdge(head=node1,tail=node2, weight=7, type="simple")
        edge23 = graph.AddEdge(head=node2,tail=node3, weight=10, type="simple")
        edge24 = graph.AddEdge(head=node2,tail=node2, weight=15, type="simple")
        edge34 = graph.AddEdge(head=node3,tail=node4, weight=11, type="simple")
        edge45 = graph.AddEdge(head=node4,tail=node5, weight=6, type="simple")

	graph.close()


def Queries():
	##create instance for graphAPI for mongodb
        graph = GraphMongo('localhost', 27018)

	print "##### GET NODES AND NEIGHTBOURS #####"
	print "\nGet one node"
	nodes = graph.GetNodes(weight=6)
	print set(nodes)

	print "\nGet all nodes"
        nodes = graph.GetNodes()
        print set(nodes)

	print "\nGet nodes by quering"
	nodes = graph.GetNodes(query={"weight":{"$in":[5,6]}})
	print set(nodes)

        print "\nFetch nodes from a list"
        fetched = nodes.Fetch()
        print fetched

        print "\nget related nodes by nodes"
	print "\nget related nodes given a list of nodes"
	nodes= graph.GetNodes(query={"weight":{"$in":[5,6]}})
	relatednodes = nodes.GetNeighbours() 
        print set(relatednodes)
	relatednodes = nodes.GetNeighbours()
        print set(relatednodes)
	relatednodes = graph.GetNeighbours(nodes=nodes)
        print set(relatednodes)
	
        print "\nget related nodes of related nodes"
	relatednodes = nodes.GetNeighbours()
	relatednodes = relatednodes.GetNeighbours() ###add disjunction, remove previous nodes
	print set(relatednodes)
	relatednodes = nodes.GetNeighbours().GetNeighbours()
	print set(relatednodes)

        print "\nget related nodes + related nodes of related nodes"
        relatednodes = nodes.GetNeighbours()
	relatednodes = relatednodes.GetNeighbours(disjunction=["node"]) ###add disjunction, remove previous nodes
	print set(relatednodes)

	print "\nget nodes + related nodes + related nodes of related nodes"
        relatednodes = nodes.GetNeighbours(disjunction=["node"]).GetNeighbours() ###add disjunction, remove previous nodes
        print relatednodes.Fetch()

	print "\nget related nodes given a list of nodes and the weight of the edges"
        docs = nodes.GetNeighbours(weight=6)
        print set(docs)
	print "\nget related nodes given a list of nodes and query for the weight of the edges"
        docs = nodes.GetNeighbours(query={"weight":{"$in":[2,14,6]}})
        print set(docs)

	print "\nget nodes FROM given a list of nodes and weight, in a directed graph"
	docs = nodes.GetNeighbours(weight=6,direction="head")
        print set(docs)
        print "\nget nodes TO given a list of nodes and weight, in a directed graph"
        docs = graph.GetNeighbours(weight=6,direction="tail")
        print set(docs)
       
	print "\nget nodes FROM given a weight, in a directed graph"	
	docs = graph.GetNeighbours(weight=6,direction="head")
        print set(docs)
        print "\nget nodes TO given a weight, in a directed graph"
        docs = graph.GetNeighbours(weight=6,direction="tail")
        print set(docs)
	print "\nget nodes FROM given a query by weight, in a directed graph"
        docs = graph.GetNeighbours(query={"weight":{"$in":[6,15]}},direction="head")
        print set(docs)
	
	print "\nget edges by weight"
	edges = graph.GetEdges(weight=6)
	print set(edges)
        print "\nget edges quering by weight"
        edges = graph.GetEdges(query={"weight":6})
        print set(edges)
	
        print "\nget related nodes by edges"
        doc = graph.GetNeighbours(edges=edges)
        print set(doc)
        doc = edges.GetNeighbours()
	print set(doc)
	
	print "\nget nodes FROM by edges"
        doc = graph.GetNeighbours(edges=edges, direction="head")
        print set(doc)
        print "\nget nodes TO by edges"
        doc = graph.GetNeighbours(edges=edges, direction="tail")
        print set(doc)

	print "\nFetch edges from a list"
	fetched = edges.Fetch(type="edge")
	print fetched
	fetched = graph.Fetch(elems=edges, type="edge")
	print fetched

	print "\nGet nodes given list of nodes and list of edges"
	doc = nodes.GetNeighbours(edges=edges)
	print set(doc)

	##hola	
	nodes = graph.GetNodes(weight=6)
	print set(nodes)
	nodes1 = nodes.GetNeighbours()
	print set(nodes1)
	nodes1 = nodes.GetNeighbours(disjunction=["nodes","accumulated"])
        print set(nodes1)

	graph.close()

def Metrics():
        ##create instance for graphAPI for mongodb
        graph = GraphMongo('localhost', 27018)

	print "##### Basic Measures #####"
	print "\nVertex Count"
	vertex = graph.VertexCount()
	print vertex

	print "\nEdges Count"
	edges = graph.EdgeCount()
	print edges

	print "\n#####Degree Measures #####"
	print "\nVertex Degree"
	vd = graph.VertexDegree()
	print vd
	nodes = graph.GetNodes()
	print "\nVertex degree for a node"
	vd = graph.VertexDegree(nodes=[list(set(nodes))[0]])
	print vd
	print "\nVertex degree for a list of nodes"
	vd = graph.VertexDegree(nodes=nodes)
	print vd

        print "\nGraph distances unweighted graph using BreadthFirstSearch"
        source = graph.GetNodes(weight=6)
        target = graph.GetNodes(weight=4)
        vd = graph.GraphDistance(sources=source,targets=target)
        source = list(set(source))
        target = list(set(target))
        distance = vd[source[0]]["distance"]
        print "distance: between {0} and {1} is {2}".format(source[0],target[0],distance[target[0]])

        print "shortest path"
        path = vd[source[0]]["from"]
        item = target[0]
        while item != source[0]:
                print "node: ",item,", with distance: ",distance[item]
                item = path[item]
        print "node: ",item,", with distance: ",distance[item]


	print "\nGraph distances weighted grapg using Dijkstra"
	source = graph.GetNodes(weight=6)
	target = graph.GetNodes(weight=4)
	vd = graph.GraphDistance(sources=source,targets=target,algorithm=graph.Dijkstra)
	source = list(set(source))
        target = list(set(target))
	distance = vd[source[0]]["distance"]
	print "distance: between {0} and {1} is {2}".format(source[0],target[0],distance[target[0]])

	print "shortest path"
	path = vd[source[0]]["from"]
	item = target[0]
	while item != source[0]:
		print "node: ",item,", with distance: ",distance[item] 
		item = path[item]
	print "node: ",item,", with distance: ",distance[item]
	
	graph.close()

if __name__ == '__main__':

        parser = argparse.ArgumentParser(description="Testing GraphMongo API")
        parser.add_argument("-c","--create",required=False)
        parser.add_argument("-t","--test",required=False, help="test options values [m,q] -> ex: mq")
        args=parser.parse_args()

	if args.create is not None and args.create == 'True':
		print "******************************** CREATING GRAPH ********************************"
		CreateDirectedGraph()

        if args.test is not None and "m" in args.test:
                print "\n************************* TESTING METRICS AND MEASURES **************************"
                Metrics()
	
	if args.test is not None and "q" in args.test:
		print "\n******************************** TESTING QUERIES ********************************"
		Queries()
	
	if args.test is not None and "p" in args.test:
		print "\n******************************** TESTING PIPELINE ********************************"
		graph = GraphMongo(address='localhost', port=27018)
		elems = graph.GetNodes().GetNeighbours().Fetch()
		print elems	
