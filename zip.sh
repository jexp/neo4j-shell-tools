mvn clean install dependency:copy-dependencies
zip -j neo4j-shell-tools-2.0.zip target/import-tools-2.0-SNAPSHOT.jar target/dependency/opencsv-2.3.jar target/dependency/neo4j-geoff-1.7-SNAPSHOT.jar target/dependency/mapdb-0.9.3.jar 
s3cmd put -P neo4j-shell-tools*.zip s3://dist.neo4j.org/jexp/shell/
