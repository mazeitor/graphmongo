###import pymongo api
import pymongo
from pymongo import MongoClient
from bson.dbref import DBRef
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

	def AddNode(self,node=None, label=None, data=None):
		'''
		@brief: create a new node in the ddbb
		@param node: dictionary with a node definition, params: _id, label and data
		@param label: type name of the node
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

			if data is not None:
				node["data"]=data

			self[self._ddbb][self._node].insert(node)
			return node
		except:
			return {"status":"ko"}

	def AddEdge(self, edge=None, label=None, head=None, tail=None, data=None):
		'''
		@brief: create a new edge in the ddbb
		@param node: dictionary with an edge definition, params: _id, label, head (node 1), tail(node 2) and data
		@param label: relation name of the edge
		@param data: extra information
		@return: edge created in the ddbb, otherwise an error is returned as dictionary with status ko
        	'''
		try:
			if edge is None:
				edge = {}
				edge["_id"]=None
				

			if edge["_id"] is None:
				edge["_id"]=ObjectId()

			if label is None:
				label = "default"
			if "label" not in edge.keys() or edge["label"] is None:
				edge["label"]=label

			if data is not None:
				node["data"]=data

			if head is not None and tail is not None:
				headref = DBRef(collection = "node", id = head["_id"], label = head["label"])
				tailref = DBRef(collection = "node", id = tail["_id"], label = tail["label"]) 

				edge["head"]=headref
				edge["tail"]=tailref

			self[self._ddbb][self._edge].insert(edge)
			return edge
		except:
			return {"status":"ko"}


	def RemoveNode(self, node=None):
		'''
		@brief: remove an specific node from the ddbb
		@param node: dictionary with an edge definition, params: _id, label, head (node 1), tail(node 2) and data
		@return: dictionary with status 'ok' or 'ko' 
		'''

                if node is None or "_id" not in node.keys():
                        return {"status":"ko"}

		try:
			self[self._ddbb][self._node].remove(node)

			###removes edges in and out
			ref = DBRef(collection = "node", id = node["_id"], label = node["label"])
			head = {"head" : ref}
			tail = {"tail" : ref}
		
			self[self._ddbb][self._edge].remove(head)	
			self[self._ddbb][self._edge].remove(tail)	
			return {"status":"ok"}
		except:
			return {"status":"ko"}

	def RemoveEdge(self, edge=None):
		'''
		@brief: remove an specific edge from the ddbb
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

	def __fetch(self):
		pass


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


	def __Get(self, elems=None, type=None, query=None, projection=None, sort=None, paging=None):
		'''
		@brief: generic get function to get an element from the ddbb with a parametrized resource, node or edge
		@param elem: element to search from ddbb
		@param type: type element, node or edge
		@param expression: mongodb expression query applyed in the ddbb
		@return: list of nodes, otherwise an error is returned as dictionary with status ko
		'''
		try:	
			if projection is None:
				projection = {}

			###at the moment the query result applied is or by list of id's or the query passed as a parameter

			if query is None:
				query = {}
				
				ids = []
				for elem in elems:
					### external query, using mongo expression
					if elem["_id"] is not None:
						ids.append(elem["_id"])

				###if there is no id to get, then we will get everything
				if len(ids) > 0:
					query["_id"]={}
					query["_id"]["$in"]=ids		

			if type is None:
				return {"status":"ko"}
			else:
				elems = self[self._ddbb][type].find(query,projection)
		
			###sorting
			if sort is not None:
				elems = self.__sort(elems,sort)
		
			###paging
			if paging is not None:
				elems = self.__paging(elems, page, per_page)

			return list(elems)
		except:
			return {"status":"ko"}


	def GetNodes(self, nodes=None, label=None):
	        '''
                @brief: get list of nodes given id's or nodes related with the nodes given the label
                @param nodes: list of nodes to fetch
                @param label: 
                @return: list of nodes, otherwise an error is returned as dictionary with status ko
                '''

		### if label is not specify means you want to fetch nodes
		if label is None:
			return self.__Get(elems=nodes,type="node")
		else: ### if is specified you wanna fetch the related nodes
			if label == "*": 
				label = None

			elems = []
			for node in nodes:
				elems = elems + self.__GetNodeNeighbours(node=node,label=label)
			return elems

	def GetEdges(self, edge=None, node1=None, node2=None, label=None, head=None, tail=None):

		if edge is not None:
			return self.__Get(elem=edge,type="edge")
		elif node1 is not None and node2 is None:
			__GetEdgeNeighbours(node1=node1, node2=node2)	
			pass
		elif node1 is None and node2 is not None:
			###TODO
			pass
		elif node1 is not None and node2 is not None:
			###TODO
			pass
		elif node1 is None and node2 is None and label is not None:
			###TODO
			pass
		else:
			return {"ko"}
			pass


	def __GetEdgeNeighbours(self):
		###TODO
		pass

	def __GetNodeNeighbours__deprecated(self, node=None, label=None, head=True, tail=None):
		
                if node is None or "_id" not in node.keys():
			return {"ko"}	

		doc = [] 
		if head == True or (head is None and tail is None):
			ref = DBRef(collection = "node", id = node["_id"], label = node["label"])
			head = {"head" : ref}

			if label is not None:
				head["label"] = label

			tails = self[self._ddbb][self._edge].find(head,{"tail":1, "_id":0})

			for tail in tails:
				node = tail["tail"]
				node = self[self._ddbb][self._node].find({"_id":node.id})
				doc.append(node[0])

		if tail == True:
			pass ###TODO

		return doc


        def __GetNodeNeighbours(self, node=None, label=None, head=True, tail=None):

                if node is None or "_id" not in node.keys():
                        return {"ko"}

                doc = []
                if head == True or (head is None and tail is None):
                        ref = DBRef(collection = "node", id = node["_id"], label = node["label"])
                        head = {"head" : ref}

                        if label is not None:
                                head["label"] = label

                        tails = self[self._ddbb][self._edge].find(head,{"tail":1, "_id":0})

                        for tail in tails:
                                node = tail["tail"]
                                node = self[self._ddbb][self._node].find({"_id":node.id})
                                doc.append(node[0])

                if tail == True:
                        pass ###TODO

                return doc


######################## main test ########################
		
###test 0 basic
###connect to database
graph = GraphMongo('localhost', 27018)
print graph
import pdb
#pdb.set_trace()

print "nodes"
node1 = graph.AddNode(label="plate")
node2 = graph.AddNode(label="sample")
node3 = graph.AddNode(label="well")
node4 = graph.AddNode(label="sample")

print node1
print node2
print node3
print node4

print "edges"
edge1 = graph.AddEdge(head=node1,tail=node2, label="has_sample") 
edge2 = graph.AddEdge(head=node1,tail=node3, label="has_well") 
edge3 = graph.AddEdge(head=node2,tail=node4, label="has_sample") 
print edge1
print edge2
#print "removes edges"
#graph.RemoveEdge(edge=edge1)

#print "removes nodes"
#graph.RemoveNode(node=node2)

print "get nodes"
nodelist = [node1]
doc = graph.GetNodes(nodes=nodelist)
print doc
 
print "get neighbour"
nodelist = [node1]
docs = graph.GetNodes(nodes=nodelist,label="has_sample")
print docs
docs = graph.GetNodes(nodes=nodelist,label="*")
print docs
nodelist = [node1,node2]
docs = graph.GetNodes(nodes=nodelist,label="*")
print docs
docs = graph.GetNodes(nodes=nodelist,label="has_sample")
print docs
