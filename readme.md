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

`import-cypher [-g in.geoff]`

- -g in.geoff: newline separated geoff rule file

Example input file: in.geoff

````
(A) {"name": "Alice"}
(B) {"name": "Bob"}
(A)-[r:KNOWS]->(B)
````

Running the command:

`import-cypher -i in.geoff`

Output

`Geoff statement execution created 3 entities.`


Installation:

````
mvn clean package dependency:copy-dependencies

cp target/import-tools-1.0-SNAPSHOT.jar target/dependency/opencsv-2.3.jar /path/to/neo/lib/
````

or make those two files available on your neo4j-shell classpath
