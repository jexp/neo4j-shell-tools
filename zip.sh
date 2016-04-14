export VERSION=2.3.3
rm neo4j-shell-tools_*.zip
mvn clean install dependency:copy-dependencies
zip -j neo4j-shell-tools_$VERSION.zip target/import-tools-${VERSION}*.jar target/dependency/opencsv-2.3.jar target/dependency/geoff-0.5.0.jar target/dependency/mapdb-0.9.3.jar 
s3cmd put -P neo4j-shell-tools*.zip s3://dist.neo4j.org/jexp/shell/
