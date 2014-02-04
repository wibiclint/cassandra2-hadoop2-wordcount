package org.kiji.wordcount;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reducer class for counting words in Sherlock Holmes books.
 */
public class SherlockReducer
    extends Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {

  // Create a map from primary key names to primary key values.
  // e.g., if you are writing the count for the number of appearances of "Holmes," you would add
  // an entry in this map from CHARACTER_COL_NAME ("name") to the reducer key, "Holmes".
  private Map<String, ByteBuffer> primaryKeys;

  /**
   * Initialize the Cassandra / Hadoop reducer (create the primary key map).
   *
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  protected void setup(Context context) throws IOException, InterruptedException {
    primaryKeys = new LinkedHashMap<String, ByteBuffer>();
  }

  /**
   * Main reduce function.  Standard Hadoop word-count boilerplate, but populate a map of primary
   * characterName characterNameCounts and a list of column characterNameCounts to write to the context.
   *
   * @param characterName Character name (the "word" in word count).
   * @param characterNameCounts Counts for the character ("count" in word count).
   * @param context Hadoop job context.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void reduce(
      Text characterName,
      Iterable<IntWritable> characterNameCounts,
      Context context) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : characterNameCounts) {
      sum += val.get();
    }

    // Update the PRIMARY KEY for this INSERT.
    primaryKeys.put(SherlockStuff.CHARACTER_COL_NAME, ByteBufferUtil.bytes(characterName.toString()));

    // Create a list of the bound variables for the CQL3 INSERT query.
    List<ByteBuffer> boundVariables = new ArrayList<ByteBuffer>();
    boundVariables.add(ByteBufferUtil.bytes(sum));

    context.write(primaryKeys, boundVariables);
  }

}
