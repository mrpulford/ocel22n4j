# sqlite2n4j
This is a small ETL tool to extract OCEL 2.0 data from an sqlite database file and make it available in a neo4j database.

usage: 
'sqlite2n4j <sqlite file> host=<databasehost> user=<username> pass=<supersecretpassword>'

Afterwards, Objects and Events will be available as Nodes, connected by their respective relations.
