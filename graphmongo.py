'''
Copyright 2016, Oriol Mazariegos Canellas <oriol.mazariegos@gmail.com> 
 
This file is part of the GraphMongo framework.

GraphMongo is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

GraphMongo is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more denode_bs.

You should have received a copy of the GNU General Public License
along with GraphMongo.  If not, see <http://www.gnu.org/licenses/>.
'''

'''
Created on 01 July 2016
@author: oriol mazariegos
@copyright: Copyright 2016, GraphMongo
@credits: ["Oriol Mazariegos"]
@license: GPL
@version: 1.0.0
@maintainer: oriol mazariegos
@email: oriol.mazariegos@gmail.com
@status: production

'''

import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import os 
import random
import math
import time
import argparse
import heapq
import pdb
import Queue
import xml.etree.ElementTree as ET

class Utils():
	'''
	Utils class for check, validations and wrappers
	'''
	
	dictionary = {	"non-directed":"undirected",
			"directed":"directed",
			"node_a":"source","node_b":"target",
			"_id":"id"}

	@classmethod
	def WrapElems(self,elem):
		'''
		@brief: change elem type to list of ObjectId's
		@param elem: object with elements
		@return: list of ObjectId's 
		'''
               	if isinstance(elem,GraphMongo):
                       	elem=elem[:]
               	elif isinstance(elem,set):
                        elem=list(elem)
		elif not isinstance(elem,list) and elem is not None:
                       	elem=[elem]

      		return elem

	@classmethod
	def Type(self,type,value):
                '''
                @brief: change elem type according to type string value
                @param type: string type value
                @param value: string value
                @return: value casted to correct type
                '''
		if type == "string":
			value = str(value)
		elif type == "float":
			value = float(value)			
		elif type == "int":
			value = int(value)			
		return value

	@classmethod
        def GetDictionary(self):
                '''
                @brief: get graph dictionary
                @return: graph concepts dictionary 
                '''
		return self.dictionary

class GraphMongo(MongoClient, list):
	'''
	Graph class for mongodb database
	'''
	
	address = "localhost"  	###databse ip address
	port = 27017           	###databae listen port

	_ddbb = "graph"        	###database name
	_type = "directed"      ###graph type
	_node = "node"         	###collection name for nodes
	_edge = "edge"         	###collection name for edges
	_metadata = "metadata"  ###collection name for metadata

	_accumulated = list() 	###adding nodes from previous queries
	_pipetype = "node" 	###pipe type for pipe feature of some functions

	_schema = {	"_id" :		{"name":"id","type":"string","for":["node","edge"]},
			"weight" :	{"name":"weight","type":"int","for":["node","edge"]},
			"node_a" :	{"name":"source","type":"string","for":["edge"]},
			"node_b" :	{"name":"target","type":"string","for":["edge"]},
			"label" :	{"name":"label","type":"string","for":["node","edge"]},
			"data" :	{"name":"data","type":"string","for":["node","edge"]}}

	def __init__(self, address="localhost", port=27017, dbname="graph", type=None, elems=None, connection=True):
		''' 
        	@brief: init a connection with mongo ddbb
        	@param address: ip address where the database is located 
        	@param port: port where the database is listening
		@param dbname: name for the graph database
		@param type: graph type name ["directed","non-directed"]
                @param elems: list of ObjectId to initialize the instance with previous queries 
		'''
		self.SetParameters(address,port,dbname,type,elems)
		if connection == True:
			super(GraphMongo,self).__init__(self.address,self.port)
		

        def SetParameters(self, address=None, port=None, dbname=None, type=None, elems=None, pipetype=None):
                '''
                @brief: set parameters like address and port of the mongo instance, name of the database and results of previous query
                @param address: ip address where the database is located
                @param port: port where the database is listening
                @param dbname: name for the graph database
		@param type: graph type name ["directed","non-directed"]
                @param elems: list of ObjectId to initialize the instance with previous queries
                '''
                if address is not None:
                        self.address = address
                if port is not None:
                        self.port = port
                if address is not None or port is not None:
                        MongoClient.__init__(self,self.address,self.port)
                if dbname is not None:
                        self._ddbb = dbname
		aux = self[self._ddbb][self._metadata].find({})
		if type is not None:
			self._type = type
			self[self._ddbb][self._metadata].remove()
			self[self._ddbb][self._metadata].insert_one({"type":self._type})
		elif type is None and (aux is None or aux.count()==0):
			self[self._ddbb][self._metadata].insert_one({"type":self._type})
		else:
			self._type = list(aux)[0]["type"]
                if elems is not None:
			elements = Utils.WrapElems(elems)
                        del self[:]
                        self[:] = elements[:]
			if isinstance(elems,GraphMongo):
				self._pipetype = elems._pipetype
			if pipetype is not None:
				self._pipetype = pipetype


	def _CopyObject(self):
		'''
		@brief: copy full object
		@return: GraphMongo element
		'''
		##don't need to open a new connection because was already opened
		graph = GraphMongo(address=self.address, port=self.port, dbname=self._ddbb, type=self._type, elems=self, connection=False) 
		graph._accumulated = self._accumulated	
		graph._pipetype = self._pipetype
		graph._type = self._type
		graph._ddbb = self._ddbb

		return graph

	
	def _Reset(self):
		'''
		@brief: remove current and accumulated results come from previous queries
		@return: graphmongo object
		'''
		##remove current values
		del self[:]
		##remove accumulated values
		self._accumulated = list()
	
		aux = self._CopyObject() ###pipeline method
		return aux

        
	def ClearGraph(self, nodes=True, edges=True, metadata=False):
                '''
                @brief: remove all nodes and edges of the ddbb taking account the params
                @param nodes: option to delete all nodes
                @param edges: option to delete all edges
                @param metadata: option to delete metadata
                '''
                if nodes:
                        self[self._ddbb][self._node].remove()
                if edges:
                        self[self._ddbb][self._edge].remove()
		if metadata:
			self[self._ddbb][self._metadata].remove()	

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

	def AddEdge(self, edge=None, label=None, weight=None, node_a=None, node_b=None, data=None):
		'''
		@brief: create a new edge in the ddbb
		@param edge: dictionary with an edge definition, params: _id, label, node_a (node 1), node_b(node 2) and data
		@param label: relation name of the edge
                @param weight: weight of the edge
		@param data: extra information
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
		
			if node_a is not None and node_b is not None:
				if type == "simple":
					pass ##TODO simple and bidirectional graph
				else:
					node_aref = {"_id" : node_a["_id"]}
                        		node_bref = {"_id" : node_b["_id"]}
			
				edge["node_a"]=node_aref
                                edge["node_b"]=node_bref
	
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
                @brief: update desired edge, _id, node_a and node_b cannot be updated, remove edge and create another one
                @param edge: dictionary with an edge definition, params: _id, label, node_a (node 1), node_b(node 2) and data
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
			node_a = {"node_a" : ref}
			node_b = {"node_b" : ref}
		
			self[self._ddbb][self._edge].remove(node_a)	
			self[self._ddbb][self._edge].remove(node_b)	
			return {"status":"ok"}
		except:
			return {"status":"ko"}

	def RemoveEdge(self, edge=None):
		'''
		@brief: remove an specific edge node_a the ddbb
		@param edge: dictionary with an edge definition, params: _id, label, node_a (node 1), node_b(node 2) and data
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
                @param elem: element search node_a ddbb
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
				elems = self[:]
			else:
				elems = Utils.WrapElems(elems)# and isinstance(elems,set):

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
		@brief: generic get function get an element node_a the ddbb with a parametrized resource, node or edge
		@param elems: list of ObjectId's 
		@param type: type element, node or edge
		@param query: mongodb expression query applyed in the ddbb
		@return: list of nodes, otherwise an error is returned as dictionary with status ko
		'''
		try:	
			###at the moment the query result applied is or by list of id's or the query passed as a parameter
			if query is None:
				query = {}
				
				###if there is no id node_b get, then we will get everything
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


	def GetEdges(self, edges=None, node_a=None, node_b=None, label=None, weight=None, direction=None, query=None):
		'''
                @brief: get list of edges given id's, label or quering
                @param edges: list of edges
		@param label: label of the node or relation
                @param weight: weight of the node or relation
                @param direction: direction of the relation, "node_a|node_b"
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
			
                        if node_a is not None:
				node_a = Utils.WrapElems(node_a)
				query.update({"node_a._id" : {"$in" : node_a}})
			if node_b is not None:
                                node_b = Utils.WrapElems(node_b)
				query.update({"node_b._id" : {"$in" : node_b}})
                        
			elems = set(self.__Get(elems=edges, type="edge", query=query))

                        ##defining pipeline method output
			self._pipetype = "edge"
                        aux = self._CopyObject()
                        aux.SetParameters(elems=elems,pipetype=self._pipetype)
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
			
			elems = self.__Get(type="node", query=query)

			##defining pipeline method output
			self._pipetype = "node"
			aux = self._CopyObject() 
			aux.SetParameters(elems=elems,pipetype=self._pipetype)
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
                @param direction: direction of the relation, "node_a|node_b"
                @param query: mongodb expression query applyed in the ddbb
		@param disjunction: option to delete in the result the previous queries
                @return: graphmongo instance with list of nodes, otherwise an error is returned as dictionary with status ko
                '''
                elems=[]

		
		#param manager for pipeline method
		if nodes is None:
			if self[:] is not None and len(self)>0 and self._pipetype=="node":
				nodes = self[:]
		if edges is None:
			if self[:] is not None and len(self)>0 and self._pipetype=="edge":
				edges = self[:]
	
		if nodes is None and edges is None:
			nodes = list()	
		if nodes is None and edges is not None:
			nodes = list()
			edges = Utils.WrapElems(edges)
			if self._type == "non-directed":
				aux = self.__GetNodeNeighbours(edges=edges, direction="node_a")
				aux += self.__GetNodeNeighbours(edges=edges, direction="node_b")
				elems += list(set(aux))
			else:
                        	elems += self.__GetNodeNeighbours(edges=edges, direction=direction)
		else:
			nodes = Utils.WrapElems(nodes)
			edges = Utils.WrapElems(edges)
			if self._type == "non-directed":
				aux = self.__GetNodeNeighbours(nodes=nodes, edges=edges, label=label, weight=weight, query=query, direction="node_a")
				aux += self.__GetNodeNeighbours(nodes=nodes, edges=edges, label=label, weight=weight, query=query, direction="node_b")
				elems = list(set(aux))
			else:
				elems = self.__GetNodeNeighbours(nodes=nodes, edges=edges, label=label, weight=weight, query=query, direction=direction)
              
		if disjunction is None:
			disjunction = list()	
		if "nodes" in disjunction:
			elems = list(set(elems) - set(nodes))
		if "accumulated" in disjunction:# and self._pipetype == "node":
			elems = list(set(elems) - set(self._accumulated))

		aux = self._CopyObject() ### pipeline method
		aux.SetParameters(elems=elems,pipetype=self._pipetype)
		aux._accumulated = list(set(aux._accumulated) | set(nodes)) ##we accumulated nodes and edges without distinction

		return aux


        def __GetNodeNeighbours(self, nodes=None, edges=None, label=None, weight=None, query=None, direction=None):
                '''
                @brief: get list of nodes related with the nodes given in the parameter, also label and direction can be specified
                @param nodes: list of ObjectId's of nodes node_b or node_a
		@param edges: list of ObjectId's of edges
                @param label: label of the node or relation
                @param weight: weight of the node or relation
		@param direction: direction of the relation, node_a or node_b
                @param projection: list of attributes you want retrive {"attribute_name":"false|true",...}
                @param sort: list of attributes you want sort the results in mongodb sort format, {"attributename":"ascending|descending",...}
                @param paging: page and per_page, start index page and the number of entries shown per page
                @return: list of nodes, otherwise an error is returned as dictionary with status ko
		'''

		###direction have node_b be "node_a" or "node_b" depending if we want node_b retrieve node_a or node_b
		if direction is not None and direction not in ["node_a","node_b"]:
			return {"status":"ko"}
		try:
			if direction is None:
				FROM = "node_a"	
				TO = "node_b"
			else:
				FROM = direction
				if FROM == "node_a":
					TO = "node_b"
				elif FROM == "node_b":
					TO = "node_a"

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
                if nodes is None and self[:] is not None and len(self)>0:
                        nodes = self[:]

		if nodes is None:
			nodes = list()	
			nodes = self.GetNodes()
		
		if nodes:
			nodes = Utils.WrapElems(nodes)

		for node in nodes:
		        outdegree = self.__GetNodeNeighbours(nodes=[node],direction="node_b")
	                indegree = self.__GetNodeNeighbours(nodes=[node],direction="node_a")
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
			sources = Utils.WrapElems(sources)			

                if targets:
			targets = Utils.WrapElems(targets)
		
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
                        
			for v in neighbours[:]:
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
					
			for v in neighbours[:]:
				###check if the node have been seen before
				if seen.has_key(v) and seen[v] == True:	continue
				
				###check if the distance to new node is in the dist structure
				if not dist.has_key(v):
                                        dist[v] = float('inf')

				###get distance between nodes
				edges = self.GetEdges(node_a=u,node_b=v).Fetch(type="edge")
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


	def Reader(self,doc=None,path=None,algorithm=None):
		'''
                @brief: read a document in graph format standard to translate to graphmongo database
                @param doc: document to read 
		@param path: path to the document to read
		@param algorithm: document's format to read [GraphML, GML, NCOL]
		@return: graphmongo instance with read data, otherwise dictionary with the status if was everything ok or not
                '''
		if doc is None and path is not None:
			doc = ET.parse(path) ###read document from file
			doc = ET.tostring(doc.getroot(),encoding='utf-8',method='xml')
		if doc is None:
			return {"status":"ko"}
		if algorithm is None:
			algorithm = self.GraphMLR
		return algorithm(doc)		

	def GraphMLR(self,doc):
		doc = ET.fromstring(doc)	
		schema={}
		for key in doc.findall("key"):
			attrtype = key.get("attr.type")
			name = key.get("attr.name")
			forvalue = key.get("for")
			id = key.get("id")
			object = {"id":id,"type":attrtype,"name":name,"for":forvalue}
			if id not in schema.keys():
				schema[id] = {}
			schema[id][forvalue]=object

		###graphtype
		graph = doc.find("graph")
		graphtype = graph.get("edgedefault") 
	
		##clear graph	
		self.ClearGraph(metadata=True)	
		self[self._ddbb][self._metadata].insert_one({"type":graphtype})
		###read nodes
		for node in graph.findall("node"):
			nodeaux = {}
                        id = node.get("id")
			nodeaux["_id"] = id
			attrs=[]
			for data in node.findall("data"):
				attr = {"key":data.get("key"),"value":data.text}	
				key = attr["key"]
				value = attr["value"]
				attrtype = schema[key]["node"]["type"] 
				value = Utils.Type(attrtype,value)
				nodeaux[key] = value
			self.AddNode(node=nodeaux)
		###read edges	
		for edge in graph.findall("edge"):
			edgeaux = {}
                        id = edge.get("id")
			source = edge.get("source")
			target = edge.get("target")
			edgeaux = {"_id":id,"node_a":source,"node_b":target}
			attrs=[]
			for data in edge.findall("data"):
			        attr = {"key":data.get("key"),"value":data.text}
                                key = attr["key"]
                                value = attr["value"]
                                attrtype = schema[key]["node"]["type"]
                                value = Utils.Type(attrtype,value)
                                edgeaux[key] = value
			self.AddEdge(edge=edgeaux)

	def Writer(self,path=None,algorithm=None):
                '''
                @brief: write a document in graph format standard from graphmongo database
		@param path: path to the document to write
                @param algorithm: document's format to write [GraphML, GML, NCOL]
                @return: document to write
                '''
		if algorithm is None:
			algorithm = self.GraphML

		doc = algorithm()
		if path is None:
			path = "output_graph"
		file = open(path,"w")
		file.write(doc)
		file.close()	

		return doc

        def GraphMLW(self):
                '''
                @brief: write a document in graphML format standard from graphmongo database
                @return: document to write
                '''
		dictionary = Utils.GetDictionary()
		schema = self._schema

		###construct template
                graphml = ET.Element('graphml')
                graph = ET.SubElement(graphml,'graph')
		##construct schema
		for item in schema:
			if item in ["_id","node_a","node_b"]:
				continue
			item = schema[item]
			for value in item["for"]:
				key = ET.SubElement(graphml,"key")	
				key.set("id",item["name"])
				key.set("for",value)
				key.set("attr.name",item["name"])
				key.set("attr.type",item["type"])
						
		graphtype = self[self._ddbb][self._metadata].find_one({})["type"]
                graph.set("edgedefault",dictionary[graphtype])
		elems = self[self._ddbb][self._node].find({})
            	for elem in elems:    
			node = ET.SubElement(graph,'node')
			for key in elem.keys():
				value = elem[key]
				if key in schema.keys() and key not in ["_id","node_a","node_b"]:
					data = ET.SubElement(node,'data')
					data.set("key",str(key))
					data.text=str(value)
				else:
					if key in dictionary:
						key = dictionary[key]
					node.set(key,str(value))

            	elems = self[self._ddbb][self._edge].find({})
                for elem in elems:
                        edge = ET.SubElement(graph,'edge')
                        for key in elem.keys():
                                value = elem[key]
				if key in schema.keys() and key not in ["_id","node_a","node_b"]:
                                       	data = ET.SubElement(edge,'data')
                                        data.set("key",str(key))
                                        data.text=str(value) 
				else:
					if key in ["node_a","node_b"]:
						value = value["_id"]
                                	if key in dictionary:
                                        	key = dictionary[key]
                                	edge.set(key,str(value))

                return ET.tostring(graphml,encoding='utf-8',method='xml')


def CreateDirectedGraph(name):
	##create instance for graphAPI for mongodb
        graph = GraphMongo('localhost', 27018, dbname=name, type="directed")
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
        edge65 = graph.AddEdge(node_a=node6,node_b=node5, weight=9)
        edge56 = graph.AddEdge(node_a=node5,node_b=node6, weight=9)
        
	edge61 = graph.AddEdge(node_a=node6,node_b=node1, weight=14)
        edge16 = graph.AddEdge(node_a=node1,node_b=node6, weight=14)
	
	edge63 = graph.AddEdge(node_a=node6,node_b=node3, weight=2)
        edge36 = graph.AddEdge(node_a=node3,node_b=node6, weight=2)
	
	edge13 = graph.AddEdge(node_a=node1,node_b=node3, weight=9)
        edge31 = graph.AddEdge(node_a=node3,node_b=node1, weight=9)
        
	edge12 = graph.AddEdge(node_a=node1,node_b=node2, weight=7)
        edge21 = graph.AddEdge(node_a=node2,node_b=node1, weight=7)
        
	edge23 = graph.AddEdge(node_a=node2,node_b=node3, weight=10)
        edge32 = graph.AddEdge(node_a=node3,node_b=node2, weight=10)

	edge24 = graph.AddEdge(node_a=node2,node_b=node2, weight=15)
	edge42 = graph.AddEdge(node_a=node4,node_b=node4, weight=15)
	
        edge34 = graph.AddEdge(node_a=node3,node_b=node4, weight=11)
        edge43 = graph.AddEdge(node_a=node4,node_b=node3, weight=11)
        
	edge45 = graph.AddEdge(node_a=node4,node_b=node5, weight=6)
        edge54 = graph.AddEdge(node_a=node5,node_b=node4, weight=6)
      
	graph.close() 

def CreateSimpleGraph(name):
        ##create instance for graphAPI for mongodb
        graph = GraphMongo('localhost', 27018, dbname=name, type="non-directed")
        ##remove all previous data, nodes and edges
        graph.ClearGraph()

        ##create nodes
        node6 = graph.AddNode(weight=6)
        node5 = graph.AddNode(weight=5)
        node3 = graph.AddNode(weight=3)
        node1 = graph.AddNode(weight=1)
        node2 = graph.AddNode(weight=2)
        node4 = graph.AddNode(weight=4)
        #node9 = graph.AddNode(weight=9)

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

	graph.close()


def Queries(name):
	##create instance for graphAPI for mongodb
        graph = GraphMongo('localhost', 27018, dbname=name)
	print graph._type
	
	print "##### GET NODES AND NEIGHTBOURS #####"
	print "\nGet one node"
	nodes = graph.GetNodes(weight=6)
	print nodes[:]

	print "\nGet all nodes"
        nodes = graph.GetNodes()
        print nodes[:]

	print "\nGet nodes by quering"
	nodes = graph.GetNodes(query={"weight":{"$in":[5,6]}})
	print "bu",nodes[:]
        print "\nFetch nodes from a list"
        fetched = nodes.Fetch()
        print fetched

        print "\nget related nodes by nodes"
	print "\nget related nodes given a list of nodes"
	nodes= graph.GetNodes(query={"weight":{"$in":[5,6]}})
	relatednodes = nodes.GetNeighbours() 
        print relatednodes[:]
	relatednodes = nodes.GetNeighbours()
        print relatednodes[:]
	relatednodes = graph.GetNeighbours(nodes=nodes)
        print relatednodes[:]
	
        print "\nget related nodes of related nodes"
	relatednodes = nodes.GetNeighbours()
	relatednodes = relatednodes.GetNeighbours() ###add disjunction, remove previous nodes
	print relatednodes[:]
	relatednodes = nodes.GetNeighbours().GetNeighbours()
	print relatednodes[:]

        print "\nget related nodes + related nodes of related nodes"
        relatednodes = nodes.GetNeighbours()
	relatednodes = relatednodes.GetNeighbours(disjunction=["node"]) ###add disjunction, remove previous nodes
	print relatednodes[:]

	print "\nget nodes + related nodes + related nodes of related nodes"
        relatednodes = nodes.GetNeighbours(disjunction=["node"]).GetNeighbours() ###add disjunction, remove previous nodes
        print relatednodes.Fetch()[:]

	print "\nget related nodes given a list of nodes and the weight of the edges"
        docs = nodes.GetNeighbours(weight=6)
        print docs[:]
	print "\nget related nodes given a list of nodes and query for the weight of the edges"
        docs = nodes.GetNeighbours(query={"weight":{"$in":[2,14,6]}})
        print docs[:]

	print "\nget nodes FROM given a list of nodes and weight, in a directed graph"
	docs = nodes.GetNeighbours(weight=6,direction="node_a")
        print docs[:]
        print "\nget nodes TO given a list of nodes and weight, in a directed graph"
        docs = graph.GetNeighbours(weight=6,direction="node_b")
        print docs[:]
       
	print "\nget nodes FROM given a weight, in a directed graph"	
	docs = graph.GetNeighbours(weight=6,direction="node_a")
        print docs[:]
        print "\nget nodes TO given a weight, in a directed graph"
        docs = graph.GetNeighbours(weight=6,direction="node_b")
        print docs[:]
	print "\nget nodes FROM given a query by weight, in a directed graph"
        docs = graph.GetNeighbours(query={"weight":{"$in":[6,15]}},direction="node_a")
        print docs[:]
	
	print "\nget edges by weight"
	edges = graph.GetEdges(weight=6)
	print edges[:]
        print "\nget edges quering by weight"
        edges = graph.GetEdges(query={"weight":6})
        print edges[:]
	
        print "\nget related nodes by edges"
        doc = graph.GetNeighbours(edges=edges)
        print doc[:]
        doc = edges.GetNeighbours()
	print doc[:]
	
	print "\nget nodes FROM by edges"
        doc = graph.GetNeighbours(edges=edges, direction="node_a")
        print doc[:]
        print "\nget nodes TO by edges"
        doc = graph.GetNeighbours(edges=edges, direction="node_b")
        print doc[:]

	print "\nFetch edges from a list"
	fetched = edges.Fetch(type="edge")
	print fetched[:]
	fetched = graph.Fetch(elems=edges, type="edge")
	print fetched[:]

	print "\nGet nodes given list of nodes and list of edges"
	doc = nodes.GetNeighbours(edges=edges)
	print doc[:]

	nodes = graph.GetNodes(weight=6)
	print nodes[:]
	nodes1 = nodes.GetNeighbours()
	print nodes1[:]
	nodes1 = nodes.GetNeighbours(disjunction=["nodes","accumulated"])
        print nodes1[:]

	
	print "\nPassing elements as a parameter for creating new objects"
	nodes = graph.GetNodes(query={"weight":6})
	print nodes[:]
	newnodes = GraphMongo(address="localhost",port=27018,dbname=name,elems=nodes)
	print newnodes[:]
	print nodes.GetNeighbours()[:]
	print newnodes.GetNeighbours()[:]

	graph.close()

def Metrics(name):
        ##create instance for graphAPI for mongodb
        graph = GraphMongo('localhost', 27018, dbname=name)

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
	vd = graph.VertexDegree(nodes=nodes[:][0])
	print vd
	print "\nVertex degree for a list of nodes"
	vd = graph.VertexDegree(nodes=nodes)
	print vd

        print "\nGraph distances unweighted graph using BreadthFirstSearch"
        source = graph.GetNodes(weight=6)
        target = graph.GetNodes(weight=4)
        vd = graph.GraphDistance(sources=source,targets=target)
        source = source[:]
        target = target[:]
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
	source = source[:]
        target = target[:]
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
        parser.add_argument("-n","--name",required=False, help="name of graph db ['directed-graph','non-directed=graph']")
        args=parser.parse_args()

	name=args.name

	if args.create is not None and args.create == 'True':
		print "******************************** CREATING GRAPH ********************************"
		if name == "directed-graph":
			CreateDirectedGraph(name)
		elif name == "non-directed-graph":
			CreateSimpleGraph(name)

        if args.test is not None and "m" in args.test:
                print "\n************************* TESTING METRICS AND MEASURES **************************"
                Metrics(name)
	
	if args.test is not None and "q" in args.test:
		print "\n******************************** TESTING QUERIES ********************************"
		Queries(name)
	
	if args.test is not None and "p" in args.test:
		print "\n******************************** TESTING PIPELINE ********************************"
		##get non-directed-graph from mongodb
		graph = GraphMongo(address='localhost', port=27018, dbname="non-directed-graph")
		
		##write graph to graphml file format
		doc = graph.Writer(path="graphml.xml",algorithm=graph.GraphMLW)

		##create new graph db from graphml file	
		graph = GraphMongo(address='localhost', port=27018, dbname="graphml")
		doc = graph.Reader(path="graphml.xml",algorithm=graph.GraphMLR)
