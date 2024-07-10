# sqlite2n4j
This is a small ETL tool to extract OCEL 2.0 data from an sqlite database file and make it available in a neo4j database.

## Usage 
'sqlite2n4j <sqlite file> host=<databasehost> user=<username> pass=<supersecretpassword>'

Afterwards, Objects and Events will be available as Nodes, connected by their respective relations. Event-Object relations are undirected, Object-Object Relations are directed.

## What works
* Events, Objects and their relations are transferred to the given n4j database. 
* HTTP connections


## What doesn't
* Database names are not supported since the open source n4j container only has one DB.
* Bolt connections
* Event properties are not supported - yet. Event properties are trivial since they don't have any history, so they can just be glued to their respective nodes
* Object properties are not supported and are less trivial since they encompass the history of attribute changes to an Object. The best way of doing them right now I can think of is introducing a "PROPERTIES" relation, carrying a timestamp property, that leads to a "STATE" Node with the given attributes at the given time.

## Building
The project has been built on arch linux, though it should be buildable on just about any OS that has boost and sqlite3 libraries.
The dependencies curlpp and plog are part of the source tree.
Build it using cmake.
