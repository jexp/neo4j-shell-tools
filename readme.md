# Import data into your neo4j database from the neo4j-shell command

`neo4j-shell-tools` adds a number of commands to [neo4j-shell](http://neo4j.com/docs/stable/shell.html) which easily allow import and export data into running [Neo4j](http://www.neo4j.com/) database.

### Installation

Download [neo4j-shell-tools_3.0.1.zip](http://dist.neo4j.org/jexp/shell/neo4j-shell-tools_3.0.1.zip) and extract it in your
Neo4j Server's lib directory e.g.

```
cd /path/to/neo4j-community-3.0.1
curl http://dist.neo4j.org/jexp/shell/neo4j-shell-tools_3.0.1.zip -o neo4j-shell-tools.zip
unzip neo4j-shell-tools.zip -d lib
```

#### Easier installation on Unix

The following script does the above installation automatically, and sets the `$NEO4J_HOME` path; works on Unix:

```
NEO4J_HOME=$(neo4j-shell -c 'dbinfo -g Kernel StoreDirectory' | grep -oE '\/.*lib.*?\/') && curl -Lk -o neo4j-shell-tools.zip http://dist.neo4j.org/jexp/shell/neo4j-shell-tools_3.0.1.zip && unzip neo4j-shell-tools.zip -d ${NEO4J_HOME}lib
```

### Before you start

Restart neo4j and then launch the neo4j-shell:

```
cd /path/to/neo4j-community-3.0.1
./bin/neo4j restart
./bin/neo4j-shell
```

That assumes a default neo4j instance running on port 7474. You can call `./bin/neo4j-shell --help` to get a list of other ways to connect to a neo4j instance.

### Importing workflow

Before importing data, make sure that you create the necessary [indexes or constraints](http://neo4j.com/docs/stable/cypher-schema.html) for your data, so that you can quickly access the starting points for your graph patterns afterwards.
For instance:

```
create constraint on (p:Person) assert p.email is unique;
create index on :Person(age);
```


### Import

Then choose a suitable import command, depending on how your data is structured.
* If your data is formatted as CSV and you want to use [cypher](http://neo4j.com/docs/stable/cypher-query-lang.html) statements for importing it, use the [Cypher Import](#cypher-import) command.
* If your data is in [GraphML](http://graphml.graphdrawing.org/) format, use the [GraphML Import](#graphml-import) command.
* If your data is in [Geoff](http://nigelsmall.com/geoff) format, use the [Geoff Import](#geoff-import) command.
* If your data is in Binary format, use the [Binary Import](#binary-import) command.

#### Cypher Import

Populate your database with [write clauses](http://neo4j.com/docs/stable/query-write.html) in the [cypher](http://neo4j.com/docs/stable/cypher-query-lang.html) query language.

```
$ import-cypher [-i in.csv] [-o out.csv] [-d ,] [-q] [-b 10000] create (n:#{label} {name: {name}, age: {age}}) return id(n) as id, n.name as name
```

- -i file.csv: tab or comma separated input data file (or URL), with header. Header names are used as param-names. The cypher  statement will be executed one per row
- -o file.csv: tab or comma separated output data file, all cypher result rows will be written to file, column labels become headers
- -q: input/output file with quotes
- -d delim: delim used to separate files (e.g. `-d " ", -d \t -d ,` )
- -b size: batch size for intermediate commits

Example input file: [in.csv](examples/in.csv)

```
name	age
Michael	38
Selina	15
Rana	8
Selma	5
```

Usage:

```
$ import-cypher -d"\t" -i in.csv -o out.csv create (n {name: {name}, age: {age}}) return id(n) as id, n.name as name
Query: create (n {name: {name}, age: {age}}) return id(n) as id, n.name as name; infile in.csv delim '	' quoted false outfile out.csv batch-size 20000
Import statement execution created 4 rows of output.
```

Output file: out.csv

```
id	name
1	Michael
2	Selina
3	Rana
4	Selma
```

Optionally support types for column headers: just use `prop:type` as header in your csv, e.g. `name:string,age:int`

Supported Types

* `int`
* `long`
* `double`
* `float`
* `boolean`
* `string`
* `byte`
* and arrays thereof with `<type>_array`, e.g. `int_array`

#### Geoff Import

Populate your database with [geoff](http://nigelsmall.com/geoff) - a declarative notation for representing graph data in a human-readable format.

```
$ import-geoff [-i in.geoff]
```

- -i in.geoff: newline separated geoff rule file (or URL)

Example input file: in.geoff

```
(A) {"name": "Alice"}
(B) {"name": "Bob"}
(A)-[r:KNOWS]->(B)
```

Usage:

```
$ import-geoff -i in.geoff
Geoff import of in.geoff created 3 entities.
```

#### Binary Import

Populate your database from a binary dump of a neo4j database.  Internally, Kyro is used to serialize the graph.

```
$ import-binary [-i in.bin] [-r REL_TYPE] [-b 20000] [-c]
```

- -i in.bin: binary file from previous database export
- -r REL_TYPE default relationship-type for relationships without a label attribute
- -b batch-size batch-commit size
- -c uses a cache that spills to disk for very large imports

Usage:

```
$ import-binary -b 10000 -i /tmp/export.bin -c
```

#### GraphML Import

Populate your database from [GraphML](http://graphml.graphdrawing.org/) files. GraphML is an XML file format used to describe graphs.

```
$ import-graphml [-i in.xml] [-r REL_TYPE] [-b 20000] [-c] [-t]
```

- -i in.xml: graphml file (or URL)
- -r REL_TYPE default relationship-type for relationships without a label attribute
- -t also import node labels, see the export format
- -b batch-size batch-commit size
- -c uses a cache that spills to disk for very large imports

Example input file: in.xml

```xml
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
```

Usage:

```
$ import-graphml -i in.xml
GraphML-Import file in.xml rel-type RELATED_TO batch-size 40000 use disk-cache false
0. 100%: nodes = 1 rels = 0 properties = 0 time 11 ms
Finished: nodes = 2 rels = 1 properties = 2 total time 16 ms
GraphML import created 3 entities.
```

### Export

Choose a suitable export command, depending on your requirement for the exported data

* To export your data as Cypher statements, use, use the [Cypher Export](#cypher-export) command.
* To export your data as CSV, use the [Cypher Import](#cypher-import) command with the `-o file` option which will output the results of your queries into a CSV file.
* To export your data as [GraphML](http://graphml.graphdrawing.org/), use the [GraphML Export](#graphml-export) command.
* To export your data as a binary file, use the [Export Binary](#binary-export) command.

#### Cypher Export

If you have a populated database, you can dump it completely or partially (by using a Cypher statement) to a script-file which uses Cypher and neo4j-shell commands to control the import.

The export order is:

1. Nodes batched in transactions
1. Schema information like indexes and constraints
1. Relationships by looking up nodes by primary keys and connecting them, also batched in transactions

```
$ export-cypher [-o export.cypher] [-r] [-b 10000] [MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN *]
```

- -o file.cypher: output file for dump, if left blank the output is sent to the neo4j-shell output and has to be redirected
- -b size: batch size for commit batches for nodes and relationships
- -r add all nodes of selected relationships, also add relationships between selected nodes

**Note:**
For nodes that don't have a unique constraint, a temporary label (`UNIQUE IMPORT LABEL`) and property (`UNIQUE IMPORT ID`) are added to allow later lookup in the relationship-generation section.
A constraint for `UNIQUE IMPORT LABEL`(`UNIQUE IMPORT ID`) is added too. All, the constraint, the label and the property are removed at the end of the import script again.

Example dump file:

```
$ export-cypher -r -o test.cypher match (n)-[r]->() return n,r

// create nodes
begin
CREATE (:`UNIQUE IMPORT LABEL` {`UNIQUE IMPORT ID`:0});
CREATE (:`User` {`age`:43, `name`:"User1"});
commit

// add schema
begin
CREATE INDEX ON :`User`(`age`);
CREATE CONSTRAINT ON (node:`User`) ASSERT node.`name` IS UNIQUE;
CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;
commit
schema await

// create relationships
begin
MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`User`{`name`:"User1"}) CREATE (n1)-[:`KNOWS` {`since`:2011}]->(n2);
commit

// clean up temporary import keys
begin
MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 1000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;
commit
begin
DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;
commit
```

#### Binary Export

Export your Neo4j graph database to a binary file.

```
$ export-binary -o out.bin
```

- -o out.bin

Usage:

```
$ export-binary -o out.bin
```

#### GraphML Export

Export your Neo4j graph database to [GraphML](http://graphml.graphdrawing.org/) files. GraphML is an XML file format used to describe graphs. Can be used to import and visualize your graph in [Gephi](http://gephi.org).

```
$ export-graphml [-t] [-r] [-o out.graphml] [match (n:Foo)-[r]->() return n,r]
```

- -t write types, do a first pass over the data to determine property-types and write them to the graphml header
- -r add all nodes of selected relationships
- -o out.graphml: graphml file to write to
- optional cypher query to select a subgraph to export

Example output file: out.graphml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
 http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
<graph id="G" edgedefault="directed">
<key id="name" for="node" attr.name="name" attr.type="string"/>
<key id="count" for="edge" attr.name="count" attr.type="int"/>
<node id="n0" labels=":FOO" ><data key="labels">:FOO</data><data key="name">John Doe</data></node>
<node id="n1" labels=":FOO" ><data key="labels">:FOO</data><data key="name">Jane Doe</data></node>
<edge id="e0" source="n0" target="n1" label="KNOWS"><data key="label">KNOWS</data><data key="count">0</data></edge>
<edge id="e1" source="n1" target="n0" label="KNOWS"><data key="label">KNOWS</data><data key="count">1</data></edge>
</graph>
</graphml>
```

Usage:

```
$ export-graphml -o out.graphml
```

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

```
git clone git@github.com:jexp/neo4j-shell-tools.git
cd neo4j-shell-tools
mvn clean package dependency:copy-dependencies
```

Then copy the jars that get generated into the neo4j lib directory:

```
cp target/import-tools-*.jar target/dependency/opencsv-*.jar target/dependency/geoff-*.jar target/dependency/mapdb-*.jar target/dependency/kryo-*.jar target/dependency/reflectasm-*.jar target/dependency/minlog-*.jar target/dependency/objenesis-*.jar /path/to/neo4j-community/lib
```

or make those two files available on your neo4j-shell classpath

#### Setup auto indexing

**NOTE: Using the "old" auto-index is discouraged, except if you know what you're doing. Use the label-based schema indexes instead.**

The auto index command is used to automatically create indexes on certain properties defined on nodes or relationships. This is in addition to the properties defined in 'conf/neo4j.properties'.

```
auto-index [-t Node|Relationship] [-r] name age title
```

- -r stops indexing those properties

Usage:

```
$ auto-index name age title
Enabling auto-indexing of Node properties: [name, age, title]
```
