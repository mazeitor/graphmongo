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

	def AddEdge(self, edge=None, label=None, weight=0, head=None, tail=None, data=None):
		'''
		@brief: create a new edge in the ddbb
		@param edge: dictionary with an edge definition, params: _id, label, head (node 1), tail(node 2) and data
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
				edge["data"]=data
			
			if "weight" not in edge.keys():
				edge["weight"]=weight

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
		@brief: remove an specific node head the ddbb
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


	def __Get(self, elems=None, type=None, query=None, projection=None, sort=None, paging=None):
		'''
		@brief: generic get function tail get an element head the ddbb with a parametrized resource, node or edge
		@param elem: element tail search head ddbb
		@param type: type element, node or edge
		@param query: mongodb expression query applyed in the ddbb
		@param projection: list of attributes you want retrive {"attribute_name":"false|true",...}
		@param sort: list of attributes you want sort the results in mongodb sort format, {"attributename":"ascending|descending",...}
		@param paging: page and per_page, start index page and the number of entries shown per page
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

				###if there is no id tail get, then we will get everything
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
                                page = paging["page"]
                                per_page = paging["per_page"]
				elems = self.__paging(elems, page, per_page)

			return list(elems)
		except:
			return {"status":"ko"}


	def GetNodes(self, nodes=None, label=None, direction=None, query=None, projection=None, sort=None, paging=None):
	        '''
                @brief: get list of nodes given id's or nodes related with the nodes given the label
                @param nodes: list of nodes to fetch
                @param label: label of the node or relation
		@param direction: direction of the relation, "head|tail"
                @param query: mongodb expression query applyed in the ddbb
                @param projection: list of attributes you want retrive {"attribute_name":"false|true",...}
                @param sort: list of attributes you want sort the results in mongodb sort format, {"attributename":"ascending|descending",...}
                @param paging: page and per_page, start index page and the number of entries shown per page
                @return: list of nodes, otherwise an error is returned as dictionary with status ko
                '''

		### if label and direction are specified means you wanna fetch the related nodes
		if (nodes is not None and (label is not None or direction is not None)) or (nodes is None and (label is not None and direction is not None)): 
			if label == "*": 
				label = None

			elems = []
			if nodes is None:
				elems = self.__GetNodeNeighbours(label=label,direction=direction, projection=projection, sort=sort, paging=paging)
			else:
				for node in nodes:
					elems = elems + self.__GetNodeNeighbours(node=node,label=label,direction=direction, projection=projection, sort=sort, paging=paging)
			return elems
		else: ### if label and direction are not specify means you want fetch the nodes given in the param
			if query is None and label is not None:
                                query = {}
                        	query.update({"label":label})
                        return self.__Get(elems=nodes,type="node", query=query, projection=projection, sort=sort, paging=paging)

        def __GetNodeNeighbours(self, node=None, label=None, direction=None, projection=None, sort=None, paging=None):
                '''
                @brief: get list of nodes related with the nodes given in the parameter, also label and direction can be specified
                @param nodes: list of nodes head or tail
                @param label: label of the node or relation
		@param direction: direction of the relation, head or tail
                @param projection: list of attributes you want retrive {"attribute_name":"false|true",...}
                @param sort: list of attributes you want sort the results in mongodb sort format, {"attributename":"ascending|descending",...}
                @param paging: page and per_page, start index page and the number of entries shown per page
                @return: list of nodes, otherwise an error is returned as dictionary with status ko
		'''

                #if node is None or "_id" not in node.keys():
                #        return {"status":"ko"}

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
			
			if node is not None:
				ref = DBRef(collection = "node", id = node["_id"], label = node["label"])
				nodehead = {FROM : ref}
			else:
				nodehead = {}

			if label is not None:
				nodehead["label"] = label

			nodestail = self[self._ddbb][self._edge].find(nodehead,{TO:1, "_id":0})

			ids = []
			for nodetail in nodestail:
				node = nodetail[TO]
				ids.append(node.id)

			elems = self[self._ddbb][self._node].find({"_id": {"$in":ids}})
			
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

######################## main test ########################
		
###test 0 basic
###connect tail database
graph = GraphMongo('localhost', 27018)

###create nodes
print "create nodes"
node1 = graph.AddNode(label="plate")
node2 = graph.AddNode(label="sample")
node3 = graph.AddNode(label="well")
node4 = graph.AddNode(label="sample")

print node1
print node2
print node3
print node4

print "create edges"
edge1 = graph.AddEdge(head=node1,tail=node2, label="has_sample") 
edge2 = graph.AddEdge(head=node1,tail=node3, label="has_well") 
edge3 = graph.AddEdge(head=node2,tail=node4, label="has_sample") 
print edge1
print edge2

print "removes edges"
#graph.RemoveEdge(edge=edge1)

print "removes nodes"
#graph.RemoveNode(node=node2)

print "get nodes"
print "fetch one node from a list"
nodelist = [node1]
doc = graph.GetNodes(nodes=nodelist)
print doc
 
print "query a node"
doc = graph.GetNodes(label="plate")
print doc
doc = graph.GetNodes(query={"label":"plate"})
print doc
doc = graph.GetNodes(query={"label":"plate"}, paging={"page":0,"per_page":5})
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
docs = graph.GetNodes(nodes=nodelist,label="has_sample",direction="head")
print docs
docs = graph.GetNodes(nodes=nodelist,label="has_sample",direction="tail")
print docs
docs = graph.GetNodes(label="has_sample",direction="tail")
print docs
docs = graph.GetNodes(label="has_sample",direction="tail", paging={"page":0,"per_page":5})
print docs
