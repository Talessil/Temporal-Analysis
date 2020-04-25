from neo4j import GraphDatabase
import pandas as pd
import csv

graphdb = GraphDatabase.driver(uri="bolt://localhost:7687",auth=("neo4j", "1234"))


############################################################################################################
############################################################################################################
######################      SAVE IN A CSV FILE THE RELATION NODE-CLUSTER       #############################
############################################################################################################
############################################################################################################
"""
i = ['file:/1.csv', 'file:/2.csv', 'file:/3.csv', 'file:/4.csv', 'file:/5.csv', 'file:/6.csv', 'file:/7.csv', 'file:/8.csv', 'file:/9.csv', 'file:/10.csv', 'file:/11.csv', 'file:/12.csv', 'file:/13.csv', 'file:/14.csv', 'file:/15.csv', 'file:/16.csv', 'file:/17.csv', 'file:/18.csv']
o = ['core-cluster-2017-1.csv', 'core-cluster-2017-2.csv', 'core-cluster-2017-3.csv', 'core-cluster-2017-4.csv', 'core-cluster-2017-5.csv', 'core-cluster-2017-6.csv', 'core-cluster-2017-7.csv', 'core-cluster-2017-8.csv', 'core-cluster-2017-9.csv', 'core-cluster-2017-10.csv', 'core-cluster-2017-11.csv', 'core-cluster-2017-12.csv', 'core-cluster-2018-1.csv', 'core-cluster-2018-2.csv', 'core-cluster-2018-3.csv', 'core-cluster-2018-4.csv', 'core-cluster-2018-5.csv', 'core-cluster-2018-6.csv']

for k in range(0,18):
	tupl = ()
	f =  open('/home/tales/Downloads/ASONAM/'+o[k], 'w')

	session = graphdb.session() 
	session.run("Load csv with headers from $url as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.author_id1)})  ON CREATE SET u.idpessoa = toInteger(csvline.author_id1)", url = i[k])
	session.run("Load csv with headers from $url as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.author_id2)})  ON CREATE SET u.idpessoa = toInteger(csvline.author_id2)", url = i[k])
	session.run("Load csv with headers from $url as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.author_id1)}),(b:Pessoa {idpessoa: toInteger(csvline.author_id2)}) create (b)-[r:Publicou{total:toFloat(csvline.count)}]->(a)", url = i[k])
	session.close()

	session = graphdb.session()
	session.run("CALL netscan.find_communities('Pessoa','Publicou','idpessoa','total', 1, 4, 1)")
	session.close()

	session = graphdb.session()
	a = session.run("MATCH (c:Cluster)-[]->(a:Pessoa {core: 1}) RETURN a.idpessoa,c.id")
	session.close()

	with f:
		writer = csv.writer(f)
		tupl = ("author_id","cluster_id")
		writer.writerow(tupl)
		for record in a:
			tupl = (record["a.idpessoa"],record["c.id"],)
			writer.writerow(tupl)

	session = graphdb.session()
	session.run("MATCH ()-[a]->() DELETE a")
	session.run("MATCH (a) DELETE a")
	session.close()
"""

############################################################################################################
############################################################################################################
######################      SAVE IN A CSV FILE THE TOTAL CONTRIBUTION OF EACH NODE       ###################
############################################################################################################
############################################################################################################
"""minPnt use all cores, minPnt2 use all specific cores"""
"""
i = ['file:/1.csv', 'file:/2.csv', 'file:/3.csv', 'file:/4.csv', 'file:/5.csv', 'file:/6.csv', 'file:/7.csv', 'file:/8.csv', 'file:/9.csv', 'file:/10.csv', 'file:/11.csv', 'file:/12.csv', 'file:/13.csv', 'file:/14.csv', 'file:/15.csv', 'file:/16.csv', 'file:/17.csv', 'file:/18.csv']
j = ['cores1.csv', 'cores2.csv', 'cores3.csv', 'cores4.csv', 'cores5.csv', 'cores6.csv', 'cores7.csv', 'cores8.csv', 'cores9.csv', 'cores10.csv', 'cores11.csv', 'cores12.csv', 'cores13.csv', 'cores14.csv', 'cores15.csv', 'cores16.csv', 'cores17.csv', 'cores18.csv']

f = open('/home/tales/Downloads/ASONAM/minPnt2.csv', 'w')

lista = []
for k in range(0,18):

	dados =  pd.read_csv('/home/tales/Downloads/ASONAM/csv/cores/'+j[k], sep=",", header = 0)
	
	session = graphdb.session() 
	session.run("Load csv with headers from $url as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.author_id1)})  ON CREATE SET u.idpessoa = toInteger(csvline.author_id1)", url = i[k])
	session.run("Load csv with headers from $url as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.author_id2)})  ON CREATE SET u.idpessoa = toInteger(csvline.author_id2)", url = i[k])
	session.run("Load csv with headers from $url as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.author_id1)}),(b:Pessoa {idpessoa: toInteger(csvline.author_id2)}) create (b)-[r:Publicou{total:toFloat(csvline.count)}]->(a)", url = i[k])
	session.close()
	
	tupl = ()
	for pessoa in dados.idpessoa:
		session = graphdb.session()
		a = session.run("MATCH (a:Pessoa)-[r]->(b:Pessoa {idpessoa: $id}) RETURN count(r)", id = pessoa)
		session.close()
		for record in a:
			k = record["count(r)"]
			tupl = tupl + (k,)
	lista.append(tupl)

	session = graphdb.session()
	session.run("MATCH ()-[a]->() DELETE a")
	session.run("MATCH (a) DELETE a")
	session.close()
	print(tupl)

with f:
	writer = csv.writer(f)
	for i in zip(*lista):
		writer.writerow(i)

"""


############################################################################################################
############################################################################################################
######################      SAVE IN A CSV FILE ALL CORE NODES RELATED TO A MINPNT       ####################
############################################################################################################
############################################################################################################
"""
i = ['file:/1.csv', 'file:/2.csv', 'file:/3.csv', 'file:/4.csv', 'file:/5.csv', 'file:/6.csv', 'file:/7.csv', 'file:/8.csv', 'file:/9.csv', 'file:/10.csv', 'file:/11.csv', 'file:/12.csv', 'file:/13.csv', 'file:/14.csv', 'file:/15.csv', 'file:/16.csv', 'file:/17.csv', 'file:/18.csv']
o = ['export1.csv', 'export2.csv', 'export3.csv', 'export4.csv', 'export5.csv', 'export6.csv', 'export7.csv', 'export8.csv', 'export9.csv', 'export10.csv', 'export11.csv', 'export12.csv', 'export13.csv', 'export14.csv', 'export15.csv', 'export16.csv', 'export17.csv', 'export18.csv']

for k in range(0,18):
	session = graphdb.session() 
	session.run("Load csv with headers from $url as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.author_id1)})  ON CREATE SET u.idpessoa = toInteger(csvline.author_id1)", url = i[k])
	session.run("Load csv with headers from $url as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.author_id2)})  ON CREATE SET u.idpessoa = toInteger(csvline.author_id2)", url = i[k])
	session.run("Load csv with headers from $url as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.author_id1)}),(b:Pessoa {idpessoa: toInteger(csvline.author_id2)}) create (b)-[r:Publicou{total:toFloat(csvline.count)}]->(a)", url = i[k])
	session.close()

	session = graphdb.session()
	session.run("CALL netscan.find_communities('Pessoa','Publicou','idpessoa','total', 1, 30, 1)")
	session.close()


	#session = graphdb.session()
	#session.run("MATCH (a:Pessoa {core: 1}) WITH collect(a) AS p CALL apoc.export.csv.data(p, [], $url, {}) YIELD  nodes RETURN nodes", url = o[k])
	#session.close()

	session = graphdb.session()
	session.run("MATCH ()-[a]->() DELETE a")
	session.run("MATCH (a) DELETE a")
	session.close()
"""

############################################################################################################
############################################################################################################
######################      PRINT CORE NODES RELATED TO ALL MINPNTS OF A YEAR       ########################
############################################################################################################
############################################################################################################
"""
for i in range(4,31): 

	session = graphdb.session() 
	session.run("Load csv with headers from 'file:/2018-3.csv' as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.author_id1)})  ON CREATE SET u.idpessoa = toInteger(csvline.author_id1)")
	session.run("Load csv with headers from 'file:/2018-3.csv' as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.author_id2)})  ON CREATE SET u.idpessoa = toInteger(csvline.author_id2)")
	session.run("Load csv with headers from 'file:/2018-3.csv' as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.author_id1)}),(b:Pessoa {idpessoa: toInteger(csvline.author_id2)}) create (b)-[r:Publicou{total:toFloat(csvline.count)}]->(a)")
	session.close()

	session = graphdb.session()
	session.run("CALL netscan.find_communities('Pessoa','Publicou','idpessoa','total', 1, $n, 1)", n = i)
	session.close()

	session = graphdb.session()
	q1 = "MATCH (a:Pessoa {core: 1}) RETURN count(a)"
	n = session.run(q1)
	for k in n:
		print(k)
	session.close()

	session = graphdb.session()
	session.run("MATCH ()-[a]->() DELETE a")
	session.run("MATCH (a) DELETE a")
	session.close()
"""

