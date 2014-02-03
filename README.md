## Simple word count example using Hadoop and Cassandra.

I put this example together as a simple working reference that uses Cassandra 2.0 and Hadoop 2.0.
To get this to work, I also put together a quick
[patch](https://github.com/wibiclint/cassandra2-hadoop2) for Cassandra 2.0 and Hadoop 2.0.

How to run:

#### Setup

- Download, install, and launch the [Kiji BentoBox](http://www.kiji.org/getstarted/), a standalone
  Hadoop / HBase cluster.  When running the BentoBox, ensure that you are using Java 7, not Java 6.  I had to use the
    command `JAVA_HOME=$JAVA_HOME hadoop jar target/cassandra_hadoop-1.0-SNAPSHOT.jar wc_cassandra`.
- Download, install, and launch Cassandra 2.0.

#### Build

- Check out the [C* 2.0 / Hadoop 2.0 patch](https://github.com/wibiclint/cassandra2-hadoop2) and
  install it into your local repo.
- Run `mvn package` to build the JAR file with the application.

#### Run

- Download some text files as plain text (these you'll use for wordcount).
- Load the data into the database: `java -cp target/cassandra_hadoop-1.0-SNAPSHOT.jar org.kiji.wordcount.Setup data/*.txt`

- Launch the application: `JAVA_HOME=$JAVA_HOME hadoop jar target/cassandra_hadoop-1.0-SNAPSHOT.jar <output dir>`

