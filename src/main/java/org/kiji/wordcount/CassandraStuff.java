package org.kiji.wordcount;

/**
 * Static constants for the keyspace, table, etc.
 */
public class CassandraStuff {
  public final static String KEYSPACE = "sherlock";

  //public final static String BOOK_TABLE = KEYSPACE + ".books";
  public final static String BOOK_TABLE = "books";

  public final static String BOOK_COL_TITLE = "title";

  public final static String BOOK_COL_STORY = "story";
}
