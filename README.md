# File-Blob

## Build

docker exec -ti cdap-sandbox bash


To build your plugins:

    mvn clean package
    
You can also build without running tests: 

    mvn clean package -DskipTests


Install

cdap cli load artifact target/file-blob-0.0.1-SNAPSHOT.jar config-file target/file-blob-0.0.1-SNAPSHOT.json

