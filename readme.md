A bunch of import tools for the neo4j-shell

Currently

### Auto-Index `auto-index`

Usage:

`auto-index [-t Node|Relationship] [-r] name age title` 

- -r stops indexing those properties

### Cypher Import `import-cypher`

`import-cypher [-i in.csv] [-o out.csv] [-d,] [-q] [-b 10000] create (n {name: {name}, age: {age}}) return id(n) as id, n.name as name`

- -i file.csv: tab or comma separated input data file, with header, header names are param-names, statement will be executed with each row
- -o file.csv: tab or comma separated output data file, all cypher result rows will be written to file, column labels become headers
- -q: input/output file with quotes
- -d delim: delim used to separate files
- -b size: batch size for intermediate commits
- uses opencsv-2.3.jar

Example input file: in.csv

````
name	age
Michael	38
Selina	15
Rana	8
Selma	5
````

Running the command:

`import-cypher -i in.csv -o out.csv create (n {name: {name}, age: {age}}) return id(n) as id, n.name as name`

Output

`Import statement execution created 3 rows of output.`

Output file: out.csv

````
id	name
1	Michael
2	Selina
3	Rana
4	Selma
````

### Geoff Import `import-geoff`

`import-geoff [-g in.geoff]`

- -g in.geoff: newline separated geoff rule file
- uses neo4j-geoff-1.7-SNAPSHOT.jar

Example input file: in.geoff

````
(A) {"name": "Alice"}
(B) {"name": "Bob"}
(A)-[r:KNOWS]->(B)
````

Running the command:

`import-geoff -g in.geoff`

Output

`Geoff import created 3 entities.`

### GraphML Import `import-graphml`

`import-graphml [-i in.xml] [-t REL_TYPE] [-b 20000] [-c]`

- -i in.xml: graphml file
- supports attributes, supports only single pass parsing, optimization for `parse.nodeids="canonical"`
- -t REL_TYPE default relationship-type for relationships without a label attribute
- -b batch-size batch-commit size
- -c uses a cache that spills to disk for very large imports (uses mapdb-0.9.3.jar)

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

Running the command:

`import-graphml -i in.xml`

Output

`GraphML import created 3 entities.`


#### Performance Test

I imported the Enron Dataset in GraphML that @chrisdiehl generated a while ago.
See http://www.infochimps.com/datasets/enron-email-data-with-manager-subordinate-relationship-metadata

It took 5 minutes to import on my MBA:

`Finished: nodes = 343266 rels = 1903201 properties = 8888993 total time 313491 ms`

Installation:

````
mvn clean package dependency:copy-dependencies

cp target/import-tools-1.0-SNAPSHOT.jar target/dependency/opencsv-2.3.jar target/dependency/neo4j-geoff-1.7-SNAPSHOT.jar target/dependency/mapdb-0.9.3.jar /path/to/neo/lib/
````

or make those two files available on your neo4j-shell classpath
