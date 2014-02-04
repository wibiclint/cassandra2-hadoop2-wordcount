package org.kiji.wordcount;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Reads in a Sherlock Holmes story from a Cassandra table and outputs key/value pairs of the form
 * (word, 1).
 */
public class SherlockMapper
    extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, IntWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(SherlockMapper.class);

  private final static IntWritable ONE = new IntWritable(1);
  private Text word = new Text();

  @Override
  public void map(
      Map<String, ByteBuffer> key,
      Map<String, ByteBuffer> columns,
      Context context) throws IOException, InterruptedException {

    // Get the column with the actual story in it.
    assert(columns.containsKey(SherlockStuff.BOOK_COL_STORY));

    String storyText = ByteBufferUtil.string(columns.get(SherlockStuff.BOOK_COL_STORY));
    StringTokenizer itr = new StringTokenizer(storyText);

    LOG.info("Running mapper task with story text " + storyText);

    // Loop through all of the words in the story.
    while (itr.hasMoreTokens()) {
      String nextWord = itr.nextToken();

      // Filter out words that aren't characters we care about.
      if (!SherlockStuff.CHARACTERS_OF_INTEREST.contains(nextWord)) { continue; }

      // Send this word and the count of 1 into the combiners / reducers.
      word.set(nextWord);
      context.write(word, ONE);
    }
  }
}
