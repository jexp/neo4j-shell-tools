# Import data into your neo4j database from the neo4j-shell command

neo4j-shell-tools adds a bunch of commands to [neo4j-shell](http://docs.neo4j.org/chunked/stable/shell.html) which allow you to insert data into a running [neo4j](http://www.neo4j.org/) database without any hassle.

### Installation

Download [neo4j-shell-tools-1.9.zip](http://dist.neo4j.org/jexp/shell/neo4j-shell-tools-1.9.zip) and extract it in your
neo4j server's lib directory e.g.

````
cd /path/to/neo4j-community-1.9.1
curl http://dist.neo4j.org/jexp/shell/neo4j-shell-tools-1.9.zip -o neo4j-shell-tools.zip 
unzip neo4j-shell-tools.zip -d lib
````

### Before you start

Restart neo4j and then launch the neo4j-shell:

````
cd /path/to/neo4j-community-1.9.1
./bin/neo4j restart
./bin/neo4j-shell
````

That assumes a default neo4j instance running on port 7474. You can call `./bin/neo4j-shell --help` to get a list of other ways to connect to a neo4j instance.


### Importing workflow

Before importing data, use the [Auto Index](#setup-auto-indexing) command to set up indexing so that you'll be able to find the data afterwards.

Then choose a suitable import command, depending on how your data is structured.
* If your data is formatted as [cypher](http://docs.neo4j.org/chunked/milestone/cypher-query-lang.html) statements, use the [Cypher Import](#cypher-import) command.
* If your data is in [geoff](http://nigelsmall.com/geoff) format, use the [Geoff Import](#geoff-import) command.
* If your data is in [GraphML](http://graphml.graphdrawing.org/) format, use the [GraphML Import](#graphml-import) command.

#### Setup auto indexing

The auto index command is used to automatically create indexes on certain properties defined on nodes or relationships. This is in addition to the properties defined in 'conf/neo4j.properties'.

`auto-index [-t Node|Relationship] [-r] name age title` 

- -r stops indexing those properties

Usage:

````
$ auto-index name age title
Enabling auto-indexing of Node properties: [name, age, title]
````

#### Cypher Import

Populate your database with [write clauses](http://docs.neo4j.org/chunked/milestone/query-write.html) in the [cypher](http://docs.neo4j.org/chunked/milestone/cypher-query-lang.html) query language.

`import-cypher [-i in.csv] [-o out.csv] [-d ,] [-q] [-b 10000] create (n {name: {name}, age: {age}}) return id(n) as id, n.name as name`

- -i file.csv: tab or comma separated input data file, with header. Header names are used as param-names. The cypher  statement will be executed one per row.
- -o file.csv: tab or comma separated output data file, all cypher result rows will be written to file, column labels become headers
- -q: input/output file with quotes
- -d delim: delim used to separate files (e.g. `-d " ", -d \t -d ,` )
- -b size: batch size for intermediate commits

Example input file: [in.csv](examples/in.csv)

````
name	age
Michael	38
Selina	15
Rana	8
Selma	5
````

Usage:

````
$ import-cypher -d"\t" -i in.csv -o out.csv create (n {name: {name}, age: {age}}) return id(n) as id, n.name as name
Query: create (n {name: {name}, age: {age}}) return id(n) as id, n.name as name; infile in.csv delim '	' quoted false outfile out.csv batch-size 20000
Import statement execution created 4 rows of output.
````

Output file: out.csv

````
id	name
1	Michael
2	Selina
3	Rana
4	Selma
````

#### Geoff Import

Populate your database with [geoff](http://nigelsmall.com/geoff) - a declarative notation for representing graph data in a human-readable format.

`import-geoff [-g in.geoff]`

- -g in.geoff: newline separated geoff rule file

Example input file: in.geoff

````
(A) {"name": "Alice"}
(B) {"name": "Bob"}
(A)-[r:KNOWS]->(B)
````

Usage:

````
$ import-geoff -g in.geoff
Geoff import of in.geoff created 3 entities.
````

#### GraphML Import

Populate your database from [GraphML](http://graphml.graphdrawing.org/) files. GraphML is an XML file format used to describe graphs.

`import-graphml [-i in.xml] [-t REL_TYPE] [-b 20000] [-c]`

- -i in.xml: graphml file
- -t REL_TYPE default relationship-type for relationships without a label attribute
- -b batch-size batch-commit size
- -c uses a cache that spills to disk for very large imports

Example input file: in.xml

````xml
<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
    <key id="d0" for="node" attr.name="color" attr.type="string">
        <default>yellow</default>
    </key>
    <key id="d1" for="edge" attr.name="weight" attr.type="double"/>
    <graph id="G" edgedefault="undirected">
        <node id="n0">
            <data key="d0">green</data>
        </node>
        <node id="n1"/>
        <edge id="e0" source="n0" target="n1">
            <data key="d1">1.0</data>
        </edge>
    </graph>
</graphml>
````

Usage:

````
$ import-graphml -i in.xml
GraphML-Import file in.xml rel-type RELATED_TO batch-size 40000 use disk-cache false
0. 100%: nodes = 1 rels = 0 properties = 0 time 11 ms
Finished: nodes = 2 rels = 1 properties = 2 total time 16 ms
GraphML import created 3 entities.
````

### Prerequisites

An up and running neo4j database which you can [download from here](http://www.neo4j.org/download).

### Other Technical Details

#### Libraries used
* Cypher Import uses [opencsv-2.3.jar](http://opencsv.sourceforge.net/) for parsing CSV files.
* GraphML Import uses [mapdb-0.9.3.jar](http://www.mapdb.org/) as part of the cache (-c) flag for very large imports
* Geoff Import uses [neo4j-geoff-1.7-SNAPSHOT.jar](http://nigelsmall.com/geoff)

#### More on GraphML

The 'import-graphml' command supports attributes, supports only single pass parsing, optimization for `parse.nodeids="canonical"`

An import of [@chrisdiehl](https://twitter.com/chrisdiehl)'s [Enron Dataset](http://www.infochimps.com/datasets/enron-email-data-with-manager-subordinate-relationship-metadata) took 5 minutes on a MBA:

`Finished: nodes = 343266 rels = 1903201 properties = 8888993 total time 313491 ms`

### Manual Build & Install

````
git clone git@github.com:jexp/neo4j-shell-tools.git
cd neo4j-shell-tools
mvn clean package dependency:copy-dependencies
````

Then copy the jars that get generated into the neo4j lib directory:

````
cp target/import-tools-1.0-SNAPSHOT.jar target/dependency/opencsv-2.3.jar target/dependency/neo4j-geoff-1.7-SNAPSHOT.jar target/dependency/mapdb-0.9.3.jar /path/to/neo4j-community-1.9/lib
````

or make those two files available on your neo4j-shell classpath
