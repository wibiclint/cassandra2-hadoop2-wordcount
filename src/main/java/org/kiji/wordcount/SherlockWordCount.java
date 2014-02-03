package org.kiji.wordcount;

import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.cassandra.hadoop2.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Quick and dirty Hadoop application to get started!
 */
public class SherlockWordCount extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(SherlockWordCount.class);

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.printf("Usage: hadoop jar <jar file> %s <output>\n", getClass().getSimpleName());
      System.err.println("(You don't need to indicate the main class because it is in the manifest.");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    // Boilerplate Hadoop setupSetup.
    Job job = new Job(getConf());
    job.setJarByClass(getClass());
    job.setJobName("Word count!");

    //job.setInputFormatClass(TextInputFormat.class);
    //FileInputFormat.addInputPath(job, new Path(args[0]));

    // Set up the Cassandra map-reduce stuff!!!!
    job.setInputFormatClass(CqlPagingInputFormat.class);
    ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
    ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
    ConfigHelper.setInputColumnFamily(
        job.getConfiguration(),
        CassandraStuff.KEYSPACE,
        CassandraStuff.BOOK_TABLE
    );
    ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");


    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[0]));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Set mapper, combiner, reducer classes.
    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(WordCountReducer.class);
    job.setReducerClass(WordCountReducer.class);

    LOG.info("LET'S DO THIS!!!!!!!!!!!!!");

    // Actually run the job!
    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    LOG.info("Getting started here!");
    String version = System.getProperty("java.version");
    LOG.info(version);
    int exitCode = ToolRunner.run(new SherlockWordCount(), args);
    System.exit(exitCode);
  }

}
