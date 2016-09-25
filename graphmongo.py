###import pymongo api
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import random
import math
import time

'''
Created on 01 July 2016
@author: oriol mazariegos
'''
class GraphMongo(MongoClient):
	'''
	Graph class for mongodb database
	'''
	
	address = "localhost"  ###databse ip address
	port = 27017           ###databae listen port

	_ddbb = "graph"        ###database name
	_node = "node"         ###collection name for nodes
	_edge = "edge"         ###collection name for esges

	def __init__(self, address="localhost", port=27017):
		''' 
        	@brief: init a connection with mongo ddbb
        	@param address: ip address where the database is located 
        	@param port: port where the database is listening
		'''
		MongoClient.__init__(self,address,port)

	def Clear(self, nodes=True, edges=True):
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

			##if label is not None or "label" not in edge.keys() or edge["label"] is None:
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

	def AddEdge(self, edge=None, label=None, weight=None, head=None, tail=None, data=None):
		'''
		@brief: create a new edge in the ddbb
		@param edge: dictionary with an edge definition, params: _id, label, head (node 1), tail(node 2) and data
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
			
			######if "weight" not in edge.keys():
			######	edge["weight"]=weight
                        if weight is not None:
                                edge["weight"]=weight

			if head is not None and tail is not None:
                                headref = {"_id" : head["_id"]} #"label" : head["label"]}
                                tailref = {"_id" : tail["_id"]} #"label" : tail["label"]}

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
				if len(elems) > 0:
					query["_id"]={}
					query["_id"]["$in"]=elems		

			if type is None:
				return {"status":"ko"}
			else:
				projection = {"_id":1}
				elems = self[self._ddbb][type].find(query,projection)
		
			ids = [elem["_id"] for elem in elems]
                        return ids
		except:
			return {"status":"ko"}


	def GetEdges(self, edges=None, label=None, head=None, tail=None, direction=None, query=None):
		'''
                @brief: get list of edges given id's, label or quering
                @param nodes: list of edges to fetch
                @param label: label of the node or relation
                @param direction: direction of the relation, "head|tail"
                @param query: mongodb expression query applyed in the ddbb
		@return list of edges, otherwise an error is returned as dictionary with status ko
		'''
                if query is None and label is not None:
	                query = {}
                        query.update({"label":label})
                return self.__Get(elems=edges,type="edge", query=query )


	def GetNodes(self, label=None, weight=None, direction=None, query=None):
	        '''
                @brief: get list of nodes given label, weight or other kind of query
                @param label: label of the node or relation
                @param weight: weight of the node or relation
                @param query: mongodb expression query applyed in the ddbb
                @return: list of nodes, otherwise an error is returned as dictionary with status ko
                '''
		try:
			elems=[]
			if label is not None:
				if query is None:
					query = {}
				query.update({"label" : label})

			if weight is not None:
				if query is None:
					query = {}
				query.update({"weight" : weight})
			#project = {"_id" : 1}
			#elems = self[self._ddbb][self._node].find(query,project)
			#elems = [elem["_id"] for elem in elems]
			return self.__Get(type="node", query=query)
		except:
	                return {"status":"ko"}


	def GetNeighbours(self, nodes=None, edges=None, label=None, weight=None, direction=None, query=None):
                '''
                @brief: get list of related nodes given nodes or related nodes with edges
                @param nodes: list of ObjectId's of nodes
                @param edges: list of ObjectId's of edges
                @param label: label of the node or relation
                @param weight: weight of the node or relation
                @param direction: direction of the relation, "head|tail"
                @param query: mongodb expression query applyed in the ddbb
                @return: list of nodes, otherwise an error is returned as dictionary with status ko
                '''
                elems=[]
                if edges is not None:
                        elems += self.__GetNodeNeighbours(edges=edges, direction=direction)
                else:
			elems = self.__GetNodeNeighbours(nodes=nodes, label=label, weight=weight, query=query, direction=direction)
                return elems


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
				pass
			else:		
				if query is not None:
	                                match = {"$match" : query}
                                        project = {"$project" : {"_id" : "${0}._id".format(TO)}}
                                        elems = self[self._ddbb][self._edge].aggregate([match,project])
				else:
					if nodes and any(not isinstance(node,(ObjectId)) for node in nodes):
						nodes = [node["_id"] for node in nodes]
	
					match = {"$match" : {"{0}._id".format(FROM) : {"$in" : nodes}}}
					project = {"$project" : {"_id" : "${0}._id".format(TO)}}
					elems = self[self._ddbb][self._edge].aggregate([match,project])
			ids = [elem["_id"] for elem in elems]
			return ids
		except:
			return {"status":"ko"}

def test():

	###test 0 basic
	###connect tail database
	graph = GraphMongo('localhost', 27018)
	graph.Clear()

	###create nodes
	print "create nodes"
	node1 = graph.AddNode(label="plate")
	node2 = graph.AddNode(label="sample")
	node3 = graph.AddNode(label="well")
	node4 = graph.AddNode(label="sample")
	node5 = graph.AddNode(label="sample")
	print "node1: ",node1
	print "node2: ",node2
	print "node3: ",node3
	print "node4: ",node4

	print "\ncreate edges"
	edge1 = graph.AddEdge(head=node1,tail=node2, label="has_sample") 
	edge2 = graph.AddEdge(head=node1,tail=node3, label="has_well") 
	edge3 = graph.AddEdge(head=node2,tail=node4, label="has_sample") 
	edge4 = graph.AddEdge(head=node2,tail=node3) 
	print "edge1: ",edge1
	print "edge2: ",edge2
	print "edge3: ",edge3
	print "edge4: ",edge4

	#print "\nupdate node"
	#node1["label"]="sample"
	#node1=graph.UpdateNode(node=node1)
	#print node1

	print "\nupdate edge"
	edge4["weight"]=5
	edge4=graph.UpdateEdge(edge4)
	print edge4

	#print "\nremoves edges"
	#graph.RemoveEdge(edge=edge1)

	#print "\nremoves nodes"
	#graph.RemoveNode(node=node2)

	print "\nget nodes"
	print "fetch one node from a list"
	nodelist = [node1["_id"]]
	doc = graph.Fetch(elems=nodelist)
	print doc
	 
	print "\nget nodes"
	doc = graph.GetNodes(label="plate")
	print doc
	doc = graph.GetNodes(query={"label":"plate"})
	print doc

	print "\nget related nodes by nodes"
	nodelist = [node1["_id"]]
	docs = graph.GetNeighbours(nodes=nodelist,label="has_sample")
	print docs
	docs = graph.GetNeighbours(nodes=nodelist,label="has_sample",direction="head")
	print docs
	docs = graph.GetNeighbours(nodes=nodelist,label="has_sample",direction="tail")
	print docs
	
	docs = graph.GetNeighbours(label="has_sample",direction="tail")
	print docs
	docs = graph.GetNeighbours(label="has_sample",direction="tail")
	print docs

	docs = graph.GetNeighbours(query={"weight":5},direction="head")
	print docs

	print "\nget related nodes by edges"
	edgelist = [edge1["_id"]]
	doc = graph.GetNeighbours(edges=edgelist)
	print doc
	doc = graph.GetNeighbours(edges=edgelist, direction="head")
	print doc
	doc = graph.GetNeighbours(edges=edgelist, direction="tail")
	print doc

	print "\nget edges"
	doc = graph.GetEdges(label="has_well")
	print doc
	doc = graph.GetEdges(query={"label":"has_well"})
	print doc


def CreateNodeDijkstra():

	##create instance for graphAPI for mongodb
        graph = GraphMongo('localhost', 27018)
	##remove all previous data, nodes and edges 
        graph.Clear()


	##we are gonna create the graph used to test dijkstra algorithm https://es.wikipedia.org/wiki/Algoritmo_de_Dijkstra
	
	##create nodes
        node6 = graph.AddNode(weight=6)
        node5 = graph.AddNode(weight=5)
        node3 = graph.AddNode(weight=3)
        node1 = graph.AddNode(weight=1)
        node2 = graph.AddNode(weight=2)
        node4 = graph.AddNode(weight=4)
	
	##create edges
        edge65 = graph.AddEdge(head=node6,tail=node2, weight=9)
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
        
        ##get neighbours
        nodelist = [node6["_id"]] ## or [node6]
	nodes = graph.GetNeighbours(nodes=nodelist)
	print nodes
		
	##Fetch nodes
        nodes = graph.Fetch(elems=nodes)
	print nodes


if __name__ == '__main__':

	#CreateNodeDijkstra()
	test()
