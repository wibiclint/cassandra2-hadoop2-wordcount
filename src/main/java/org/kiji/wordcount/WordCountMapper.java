package org.kiji.wordcount;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Reads in a Sherlock Holmes story from a Cassandra table and outputs key/value pairs of the form
 * (word, 1).
 */
public class WordCountMapper
    extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, IntWritable> {

  private final static IntWritable ONE = new IntWritable(1);
  private Text word = new Text();

  @Override
  public void map(
      Map<String, ByteBuffer> key,
      Map<String, ByteBuffer> columns,
      Context context) throws IOException, InterruptedException {

    // Get the column with the actual story in it.
    assert(columns.containsKey(CassandraStuff.BOOK_COL_STORY));

    String storyText = ByteBufferUtil.string(columns.get(CassandraStuff.BOOK_COL_STORY));

    StringTokenizer itr = new StringTokenizer(storyText);
    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      context.write(word, ONE);
    }
  }
}
