package org.kiji.wordcount;

import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.cassandra.hadoop2.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop2.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Quick and dirty Hadoop application to get started!
 *
 * Counts words in Sherlock Holmes books.
 */
public class SherlockCharacterCounter extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(SherlockCharacterCounter.class);

  @Override
  public int run(String[] args) throws Exception {
    /*
    if (args.length != 1) {
      System.err.printf("Usage: hadoop jar <jar file> %s <output>\n", getClass().getSimpleName());
      System.err.println("(You don't need to indicate the main class because it is in the manifest.");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    */

    // Boilerplate Hadoop setupSetup.
    Job job = new Job(getConf());
    job.setJarByClass(getClass());
    job.setJobName("Word count!");

    ////////////////////////////////////////////////////////////////////////////
    // Set Cassandra / Hadoop input options.
    job.setInputFormatClass(CqlPagingInputFormat.class);
    ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
    ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
    ConfigHelper.setInputColumnFamily(
        job.getConfiguration(),
        SherlockStuff.KEYSPACE,
        SherlockStuff.BOOK_TABLE
    );
    ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
    // The page size should be irrelevant here, since we are putting an entire document into a
    // single "text" column (shouldn't be that many total rows).
    CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "10000");
    // Keep the total number of mappers tiny --- this is just a trivial example application!
    LOG.info("Setting input split size...");
    ConfigHelper.setInputSplitSize(job.getConfiguration(), 16*1024*1024);

    ////////////////////////////////////////////////////////////////////////////
    // Set Cassandra / Hadoop output options.
    job.setOutputFormatClass(CqlOutputFormat.class);
    ConfigHelper.setOutputColumnFamily(
        job.getConfiguration(),
        SherlockStuff.KEYSPACE,
        SherlockStuff.CHARACTER_TABLE
    );

    // Set up the query for writing the table in a reducer
    // (similar to defining a "put" for HBase reducers).
    // In the reducer, we specify the PRIMARY KEY for the put.
    String putQuery = String.format(
        "UPDATE %s.%s SET %s = ?",
        SherlockStuff.KEYSPACE,
        SherlockStuff.CHARACTER_TABLE,
        SherlockStuff.CHARACTER_COL_COUNT
    );
    CqlConfigHelper.setOutputCql(job.getConfiguration(), putQuery);

    ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
    ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");

    ////////////////////////////////////////////////////////////////////////////
    // Set mapper, combiner, reducer classes.
    job.setMapperClass(SherlockMapper.class);
    // You could in theory write a version of the Reducer to use as a combiner (pretty easy for a
    // word count application).
    //job.setCombinerClass(SherlockReducer.class);
    job.setReducerClass(SherlockReducer.class);

    // TODO: Check whether we need to set these options.
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Map.class);
    job.setOutputValueClass(List.class);

    // Actually run the job!
    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    // Useful for debugging problems with running Java 6 versus Java 7.
    //String version = System.getProperty("java.version");
    //LOG.info(version);
    int exitCode = ToolRunner.run(new SherlockCharacterCounter(), args);
    System.exit(exitCode);
  }

}
