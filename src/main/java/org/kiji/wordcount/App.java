package org.kiji.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Quick and dirty Hadoop application to get started!
 */
public class App extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: hadoop jar <jar file> %s <input> <output>\n", getClass().getSimpleName());
      System.err.println("(You don't need to indicate the main class because it is in the manifest.");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    // Boilerplate Hadoop setup.
    Job job = new Job(getConf());
    job.setJarByClass(getClass());
    job.setJobName("Word count!");

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Set mapper, combiner, reducer classes.
    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(WordCountReducer.class);
    job.setReducerClass(WordCountReducer.class);

    // Actually run the job!
    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new App(), args);
    System.exit(exitCode);
  }

}
