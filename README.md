## Simple word count example using Hadoop and Cassandra.

I put this example together as a simple working reference that uses Cassandra 2.0 and Hadoop 2.0.
To get this to work, I also put together a quick
[patch](https://github.com/wibiclint/cassandra2-hadoop2) for Cassandra 2.0 and Hadoop 2.0.

To make this example epsilon more exciting than a standard word count, I assume that the inputs are
text files containing Sherlock Holmes books, and the "word count" code counts the occcurances of
different characters.

In the future, I thought it might be interesting to see, for a given character, how often he/she is
referred to by first name, last name, nickname, etc. (and to use different columns for those
occurances), but for now this is just standard word count.

How to run:

#### Setup

- Download, install, and launch the [Kiji BentoBox](http://www.kiji.org/getstarted/), a standalone
  Hadoop / HBase cluster.  When running the BentoBox, ensure that you are using Java 7, not Java 6.  I had to use the
    command `JAVA_HOME=$JAVA_HOME hadoop jar target/cassandra_hadoop-1.0-SNAPSHOT.jar wc_cassandra`.
- Download, install, and launch Cassandra 2.0.  You will almost definitely want to adjust the
  `num_tokens` entry in your `cassandra.yaml` file.  The default is 256.  This number determines the
  number of map tasks, so for small local runs, you can set this to just be 1 or something small
  like that (you may need to blow away your `data_file_direcories`, `commitlog_directory`, and
  `saved_caches_directory` after reducing the number of tokens).

#### Build

- Check out the [C* 2.0 / Hadoop 2.0 patch](https://github.com/wibiclint/cassandra2-hadoop2) and
  install it into your local repo.
- Run `mvn package` to build the JAR file with the application.

#### Run

- Download some text files as plain text (these you'll use for wordcount).
- Load the data into the database: `java -cp target/cassandra_hadoop-1.0-SNAPSHOT.jar org.kiji.wordcount.Setup data/tiny.txt`.
  You can download your own Sherlock Holmes from
  [Project Gutenberg](http://www.gutenberg.org/ebooks/author/69) or wherever and specify them at the
  command line.
- Launch the application: `JAVA_HOME=$JAVA_HOME hadoop jar target/cassandra_hadoop-1.0-SNAPSHOT.jar`


#### Take a look at the results

Open up the CQL shell and look at the results of the word count for Sherlock Holmes characters:

    cqlsh> select * from sherlock.characters ;

    name     | name_count
    ----------+------------
      Holmes |          4
      Watson |          2
    Lestrade |          1

    (3 rows)

Woo hoo!


## Notes

#### Mysery errors

I get a lot of errors like the following:

    Exception in thread "main" java.io.IOException: Could not get input splits
            at org.apache.cassandra.hadoop2.AbstractColumnFamilyInputFormat.getSplits(AbstractColumnFamilyInputFormat.java:193)

I'm not sure why these happen.  Often I run again and everything works.

#### Errors that need documentation

I got this error:

    14/02/03 15:57:02 INFO mapred.JobClient: Task Id : attempt_20140203115029605_0021_r_000000_2, Status
    : FAILED
    java.io.IOException: InvalidRequestException(why:Expected 8 or 0 byte long (1))
            at
            org.apache.cassandra.hadoop2.cql3.CqlRecordWriter$RangeClient.run(CqlRecordWriter.java:246)
            Caused by: InvalidRequestException(why:Expected 8 or 0 byte long (1))

because I was trying to write a String to a column that uses `bigint.`  I should write a wrapper
around these errors to make them more comprehensible.

#### Hanging jobs

If I try to run a job before defining a keyspace, then I see a "keyspace does not exist" (or
something like that) assertion error in the log for my Cassandra server, but my Hadoop job hangs
(instead of dying).  I need to investigate why this happens and fix it.
