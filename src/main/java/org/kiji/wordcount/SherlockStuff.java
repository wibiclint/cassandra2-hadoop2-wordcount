package org.kiji.wordcount;

import com.google.common.collect.Sets;

import java.util.HashSet;

/**
 * Static constants for the keyspace, table, etc.
 */
public class SherlockStuff {
  public final static String KEYSPACE = "sherlock";

  // All of the information for the inputs to the wordcount job.

  /** Main table with books and their text. */
  public final static String BOOK_TABLE = "books";

  /** Column in book table with titles. */
  public final static String BOOK_COL_TITLE = "title";

  /** Column in book table with the actual book text. */
  public final static String BOOK_COL_STORY = "story";

  // All of the information for the outputs of the wordcount job, in which we keep track of how many
  // times various characters are mentioned.

  /** Main table with characters and their counts. */
  public final static String CHARACTER_TABLE = "characters";

  /** Column in character table for the character name. */
  public final static String CHARACTER_COL_NAME = "name";

  /** Column in character table for the word count for the character name. */
  public final static String CHARACTER_COL_COUNT = "name_count";

  /** List of characters that we care about. */
  public final static HashSet<String> CHARACTERS_OF_INTEREST =
      Sets.newHashSet("Holmes", "Watson", "Lestrade");
}
