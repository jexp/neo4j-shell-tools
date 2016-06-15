export VERSION=3.0.3
rm neo4j-shell-tools_*.zip
mvn clean install dependency:copy-dependencies
zip -j neo4j-shell-tools_$VERSION.zip target/import-tools-${VERSION}*.jar target/dependency/opencsv-*.jar target/dependency/geoff-*.jar target/dependency/mapdb-*.jar target/dependency/kryo-*.jar target/dependency/reflectasm-*.jar target/dependency/minlog-*.jar target/dependency/objenesis-*.jar
s3cmd put -P neo4j-shell-tools*.zip s3://dist.neo4j.org/jexp/shell/
