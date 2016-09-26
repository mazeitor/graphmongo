###import pymongo api
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import random
import math
import time
import argparse


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


	def GetEdges(self, edges=None, label=None, weight=None, head=None, tail=None, direction=None, query=None):
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
                        if label is not None:
                                if query is None:
                                        query = {}
                                query.update({"label" : label})

                        if weight is not None:
                                if query is None:
                                        query = {}
                                query.update({"weight" : weight})
                        return self.__Get(elems=edges, type="edge", query=query)
                except:
                        return {"status":"ko"}


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


def CreateGraph():

	##create instance for graphAPI for mongodb
        graph = GraphMongo('localhost', 27018)
	##remove all previous data, nodes and edges 
        graph.Clear()

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
        

def Queries():
	##create instance for graphAPI for mongodb
        graph = GraphMongo('localhost', 27018)

	print "QUERING"
	print "\nGet one node"
	nodes = graph.GetNodes(weight=6)
	print nodes

	print "\nGet all nodes"
        nodes = graph.GetNodes()
        print nodes

	print "\nGet nodes by quering"
	nodes = graph.GetNodes(query={"weight":{"$in":[5,6]}})
	print nodes

        print "\nFetch nodes from a list"
        fetched = graph.Fetch(elems=nodes)
        print fetched
	
        print "\nget related nodes by nodes"
	print "\nget related nodes given a list of nodes"
        docs = graph.GetNeighbours(nodes=nodes) 
        print docs

	print "\nget related nodes given a list of nodes and the weight of the edges"
        docs = graph.GetNeighbours(nodes=nodes,weight=6)
        print docs
	print "\nget related nodes given a list of nodes and query for the weight of the edges"
        docs = graph.GetNeighbours(nodes=nodes,query={"weight":{"$in":[2,14,6]}})
        print docs

	print "\nget nodes FROM given a list of nodes and weight, in a directed graph"
	docs = graph.GetNeighbours(nodes=nodes,weight=6,direction="head")
        print docs
        print "\nget nodes TO given a list of nodes and weight, in a directed graph"
        docs = graph.GetNeighbours(nodes=nodes,weight=6,direction="tail")
        print docs
       
	print "\nget nodes FROM given a weight, in a directed graph"	
	docs = graph.GetNeighbours(weight=6,direction="head")
        print docs
        print "\nget nodes TO given a weight, in a directed graph"
        docs = graph.GetNeighbours(weight=6,direction="tail")
        print docs
	print "\nget nodes FROM given a query by weight, in a directed graph"
        docs = graph.GetNeighbours(query={"weight":{"$in":[6,15]}},direction="head")
        print docs
	
	print "\nget edges by weight"
	edges = graph.GetEdges(weight=6)
	print edges
        print "\nget edges quering by weight"
        edges = graph.GetEdges(query={"weight":6})
        print edges

        print "\nget related nodes by edges"
        doc = graph.GetNeighbours(edges=edges)
        print doc
	print "\nget nodes FROM by edges"
        doc = graph.GetNeighbours(edges=edges, direction="head")
        print doc
        print "\nget nodes TO by edges"
        doc = graph.GetNeighbours(edges=edges, direction="tail")
        print doc

	print "\nFetch edges from a list"
	fetched = graph.Fetch(elems=edges, type="edge")
	print fetched

if __name__ == '__main__':

        parser = argparse.ArgumentParser(description="Testing GraphMongo API")
        parser.add_argument("-c","--create",type=bool,required=False)
        parser.add_argument("-t","--test",type=bool,required=False)
        args=parser.parse_args()

	if args.create is not None and args.create == True:
		print "creating new graph"
		CreateGraph()
	
	if args.test is not None and args.test == True:
		print "testing graph"
		Queries()
