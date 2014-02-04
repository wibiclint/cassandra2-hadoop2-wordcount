package org.kiji.wordcount;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Set up the Cassandra database to use for testing.
 *
 * Create the keyspace, create the database, initialize with some data!
 */
public final class Setup {
  private static final Logger LOG = LoggerFactory.getLogger(Setup.class);

  /** Create the keyspace for the application. */
  private void createKeyspace(Session session) {
    // Create a keyspace, if it does not exist yet.
    session.execute(String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = " +
        "{'class': 'SimpleStrategy', 'replication_factor': 1 };",
    SherlockStuff.KEYSPACE
    ));

    session.execute("USE " + SherlockStuff.KEYSPACE);
  }

  /**
   * Set up the table for the input side of this application.
   * @param session C* session.
   * @param titlesToBooks Map from titles of books (file names) to their content.
   */
  private void setupInputTable(
      Session session,
      Map<String, String> titlesToBooks) {

    // Create the table, if it does not exist yet.
    session.execute(String.format(
        "CREATE TABLE IF NOT EXISTS %s ( " +
            "%s text PRIMARY KEY, %s text); ",
        SherlockStuff.BOOK_TABLE,
        SherlockStuff.BOOK_COL_TITLE,
        SherlockStuff.BOOK_COL_STORY
    ));

    // Populate the table with some great works of literature!
    PreparedStatement preparedStatement = session.prepare(String.format(
        "INSERT INTO %s (%s, %s) VALUES (?, ?);",
        SherlockStuff.BOOK_TABLE,
        SherlockStuff.BOOK_COL_TITLE,
        SherlockStuff.BOOK_COL_STORY
    ));

    for (Map.Entry<String, String> bookAndContents : titlesToBooks.entrySet()) {
      String bookTitle = bookAndContents.getKey();
      String bookText = bookAndContents.getValue();
      LOG.info("Inserting data for book " + bookTitle);
      session.execute(preparedStatement.bind(bookTitle, bookText));
    }
  }

  /**
   * Set up the table for the output side of this application.
   * @param session C* session.
   */
  private void setupOutputTable(Session session) {
    // Create the table, if it does not exist yet.
    session.execute(String.format(
        "CREATE TABLE IF NOT EXISTS %s ( " +
            "%s text PRIMARY KEY, %s int); ",
        SherlockStuff.CHARACTER_TABLE,
        SherlockStuff.CHARACTER_COL_NAME,
        SherlockStuff.CHARACTER_COL_COUNT
    ));
  }

  public void loadDataAndSetupTables(String bookFiles[]) {
    // Check that all of the books exists, load their contents into a hash.
    Map<String, String> titlesToBooks = new HashMap<String, String>();
    for (String bookFile : bookFiles) {
      try {
        String content = Files.toString(new File(bookFile), Charsets.UTF_8);
        LOG.info("Read book " + bookFile);
        String bookTitle = getBookTitleFromFile(bookFile);
        titlesToBooks.put(bookTitle, content);
      } catch (IOException ioe) {
        System.err.println("Problem reading file " + bookFile + "!!!");
        continue;
      }
    }

    if (0 == titlesToBooks.size()) {
      System.err.println("Please specify some books to read!");
      return;
    }

    Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    Session session = cluster.connect();
    createKeyspace(session);
    setupInputTable(session, titlesToBooks);
    setupOutputTable(session);
    cluster.shutdown();
  }

  private String getBookTitleFromFile(String bookFile) {
    //LOG.info("Getting title for book " + bookFile);
    // Sanitize the book file name to turn it into something like a title.
    String[] bookFileElements = bookFile.split("/");
    String bookFileShort = bookFileElements[bookFileElements.length - 1];
    //LOG.info("Short version of book file name is " + bookFileShort);
    String[] fileNameComponents = bookFileShort.split("\\.");
    return fileNameComponents[0];
  }

  /**
   * Actually load in the data!
   * @param args Specify the files to load.
   */
  public static void main(String args[]) {
    new Setup().loadDataAndSetupTables(args);
  }
}
