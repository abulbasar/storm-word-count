# StormWordCount
----------



## Usage
To build and run this topology, you must use Java 1.8.

### Local Mode:
    mvn clean compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=topologies.WordCountTopology
or
    mvn clean compile package && java -jar target/storm-wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar
	
### Distributed [or Cluster / Production] Mode:
Distributed mode requires a complete and proper Storm Cluster setup. 

    storm jar target/storm-wordcount-1.0-SNAPSHOT.jar topologies.WordCountTopology WordCount

