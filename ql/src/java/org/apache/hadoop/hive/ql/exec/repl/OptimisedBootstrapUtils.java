/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.event.filters.DatabaseAndTableFilter;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.parse.ReplicationSpec.getLastReplicatedStateFromParameters;
import static org.apache.hadoop.hive.ql.parse.ReplicationSpec.getTargetLastReplicatedStateFromParameters;

/**
 * Utility class for handling operations regarding optimised bootstrap in case of replication.
 */
public class OptimisedBootstrapUtils {

  /** Separator used to separate entries in the listing. */
  public static final String FILE_ENTRY_SEPARATOR = "#";
  private static final Logger LOG = LoggerFactory.getLogger(OptimisedBootstrapUtils.class);

  /** table diff directory when in progress */
  public static final String TABLE_DIFF_INPROGRESS_DIRECTORY = "table_diff";

  /** table diff directory when complete */
  public static final String TABLE_DIFF_COMPLETE_DIRECTORY = "table_diff_complete";

  /** abort Txns file which contains all the txns that needs to be aborted on new source cluster(initial target)*/
  public static final String ABORT_TXNS_FILE = "abort_txns";

  /** event ack file which contains the event id till which the cluster was last loaded. */
  public static final String EVENT_ACK_FILE = "event_ack";

  public static final String BOOTSTRAP_TABLES_LIST = "_failover_bootstrap_table_list";

  /**
   * Gets &amp; checks whether the database is target of replication.
   * @param dbName name of database
   * @param hive hive object
   * @return true, if the database has repl.target.for property set.
   * @throws HiveException
   */
  public static boolean isDbTargetOfFailover(String dbName, Hive hive) throws HiveException {
    Database database = hive.getDatabase(dbName);
    return database != null ? MetaStoreUtils.isTargetOfReplication(database) : false;
  }

  public static boolean checkFileExists(Path dumpPath, HiveConf conf, String fileName) throws IOException {
    FileSystem fs = dumpPath.getFileSystem(conf);
    return fs.exists(new Path(dumpPath, fileName));
  }

  public static void prepareAbortTxnsFile(List<NotificationEvent> notificationEvents, Set<Long> allOpenTxns,
                                          Path dumpPath, HiveConf conf) throws SemanticException {
    if (notificationEvents.size() == 0) {
      return;
    }
    Set<Long> txnsOpenedPostCurrEventId = new HashSet<>();
    MessageDeserializer deserializer = ReplUtils.getEventDeserializer(notificationEvents.get(0));
    for (NotificationEvent event: notificationEvents) {
      switch (event.getEventType()) {
        case MessageBuilder.OPEN_TXN_EVENT:
          OpenTxnMessage openTxnMessage = deserializer.getOpenTxnMessage(event.getMessage());
          txnsOpenedPostCurrEventId.addAll(openTxnMessage.getTxnIds());
          allOpenTxns.removeAll(openTxnMessage.getTxnIds());
          break;
        case MessageBuilder.ABORT_TXN_EVENT:
          AbortTxnMessage abortTxnMessage = deserializer.getAbortTxnMessage(event.getMessage());
          if (!txnsOpenedPostCurrEventId.contains(abortTxnMessage.getTxnId())) {
            allOpenTxns.add(abortTxnMessage.getTxnId());
          }
          break;
        case MessageBuilder.COMMIT_TXN_EVENT:
          CommitTxnMessage commitTxnMessage = deserializer.getCommitTxnMessage(event.getMessage());
          if (!txnsOpenedPostCurrEventId.contains(commitTxnMessage.getTxnId())) {
            allOpenTxns.add(commitTxnMessage.getTxnId());
          }
          break;
      }
    }
    if (!allOpenTxns.isEmpty()) {
      Utils.writeOutput(flattenListToString(allOpenTxns), new Path(dumpPath, ABORT_TXNS_FILE), conf);
    }
  }

  public static List<Long> getTxnIdFromAbortTxnsFile(Path dumpPath, HiveConf conf) throws IOException {
    String input;
    Path abortTxnFile = new Path(dumpPath, ABORT_TXNS_FILE);
    FileSystem fs = abortTxnFile.getFileSystem(conf);
    try (FSDataInputStream stream = fs.open(abortTxnFile);) {
      input = IOUtils.toString(stream, Charset.defaultCharset());
    }
    return unflattenListFromString(input);
  }

  private static String flattenListToString(Set<Long> list) {
    return list.stream()
            .map(Object::toString)
            .collect(Collectors.joining(FILE_ENTRY_SEPARATOR));
  }

  private static List<Long> unflattenListFromString(String input) {
    List<Long> ret = new ArrayList<>();
    for (String val : input.replaceAll(System.lineSeparator(), "").trim().split(FILE_ENTRY_SEPARATOR)) {
      ret.add(Long.parseLong(val));
    }
    return ret;
  }

  /**
   * Gets the source &amp; target event id  from the event ack file
   * @param dumpPath the dump path
   * @param conf the hive configuration
   * @return the event id from file.
   * @throws IOException
   */
  public static String[] getEventIdFromFile(Path dumpPath, HiveConf conf) throws IOException {
    String lastEventId;
    Path eventAckFilePath = new Path(dumpPath, EVENT_ACK_FILE);
    FileSystem fs = eventAckFilePath.getFileSystem(conf);
    try (FSDataInputStream stream = fs.open(eventAckFilePath);) {
      lastEventId = IOUtils.toString(stream, Charset.defaultCharset());
    }
    return lastEventId.replaceAll(System.lineSeparator(), "").trim().split(FILE_ENTRY_SEPARATOR);
  }

  /**
   * Gets the name of tables in the table diff file.
   * @param dumpPath the dump path
   * @param conf the hive configuration
   * @return Set with list of tables
   * @throws Exception
   */
  public static HashSet<String> getTablesFromTableDiffFile(Path dumpPath, HiveConf conf) throws Exception {
    FileSystem fs = dumpPath.getFileSystem(conf);
    Path tableDiffPath = new Path(dumpPath, TABLE_DIFF_COMPLETE_DIRECTORY);
    FileStatus[] list = fs.listStatus(tableDiffPath);
    HashSet<String> tables = new HashSet<>();
    for (FileStatus fStatus : list) {
      tables.add(fStatus.getPath().getName());
    }
    return tables;
  }

  /**
   * Extracts the recursive listing from the table file.
   * @param file the name of table
   * @param dumpPath the dump path
   * @param conf the hive conf
   * @return the list of paths in the table.
   * @throws IOException
   */
  public static HashSet<String> getPathsFromTableFile(String file, Path dumpPath, HiveConf conf) throws IOException {
    HashSet<String> paths = new HashSet<>();
    FileSystem fs = dumpPath.getFileSystem(conf);
    Path tableDiffPath = new Path(dumpPath, TABLE_DIFF_COMPLETE_DIRECTORY);
    Path filePath = new Path(tableDiffPath, file);
    String allEntries;
    try (FSDataInputStream stream = fs.open(filePath);) {
      allEntries = IOUtils.toString(stream, Charset.defaultCharset());
    }
    paths.addAll(Arrays.asList(allEntries.split(System.lineSeparator())).stream().filter(item -> !item.isEmpty())
        .collect(Collectors.toSet()));
    return paths;
  }

  /**
   * Gets the event id stored in database denoting the last loaded event id.
   * @param dbName the name of database
   * @param hiveDb the hive object
   * @return event id from the database
   * @throws HiveException
   */
  public static String getReplEventIdFromDatabase(String dbName, Hive hiveDb) throws HiveException {
    Database database = hiveDb.getDatabase(dbName);
    String currentLastEventId = getLastReplicatedStateFromParameters(database.getParameters());
    return currentLastEventId;
  }

  /**
   * Validates if the first incremental is done before starting optimised bootstrap
   * @param dbName name of database
   * @param hiveDb the hive object
   * @throws HiveException
   */
  public static void isFirstIncrementalPending(String dbName, Hive hiveDb) throws HiveException {
    Database database = hiveDb.getDatabase(dbName);
    if (database == null || ReplUtils.isFirstIncPending(database.getParameters()))
      throw new HiveException(
          "Replication dump not allowed for replicated database with first incremental dump pending : " + dbName);
  }

  /**
   * Creates the event ack file and sets the dump metadata post that marking completion of dump flow for first round
   * of optimised failover dump.
   * @param currentDumpPath the dump path
   * @param dmd the dump metadata
   * @param cmRoot the cmRoot
   * @param dbEventId the database event id to which we have to write in the file.
   * @param targetDbEventId the database event id with respect to target cluster.
   * @param conf the hive configuration
   * @param work the repldump work
   * @return the lastReplId denoting a fake dump(-1) always
   * @throws SemanticException
   */
  public static Long createAndGetEventAckFile(Path currentDumpPath, DumpMetaData dmd, Path cmRoot, String dbEventId,
      String targetDbEventId, HiveConf conf, ReplDumpWork work) throws Exception {
    // Keep an invalid value for lastReplId, to denote it isn't a actual dump.
    Long lastReplId = -1L;
    Path filePath = new Path(currentDumpPath, EVENT_ACK_FILE);
    Utils.writeOutput(dbEventId + FILE_ENTRY_SEPARATOR + targetDbEventId, filePath, conf);
    LOG.info("Created event_ack file at {} with source eventId {} and target eventId {}", filePath, dbEventId,
            targetDbEventId);
    work.setResultValues(Arrays.asList(currentDumpPath.toUri().toString(), String.valueOf(lastReplId)));
    long executionId = conf.getLong(Constants.SCHEDULED_QUERY_EXECUTIONID, 0L);
    dmd.setDump(DumpType.PRE_OPTIMIZED_BOOTSTRAP, work.eventFrom, lastReplId, cmRoot, executionId, false);
    dmd.write(true);
    return lastReplId;
  }

  /**
   * Returns list of notificationEvents starting from eventId that are related to the database.
   * @param eventId Starting eventId
   * @param hiveDb the hive object
   * @param work the load work
   * @throws Exception
   */
  public static List<NotificationEvent> getListOfNotificationEvents(Long eventId, Hive hiveDb,
                                                                    ReplLoadWork work) throws Exception {
    List<NotificationEvent> notificationEvents =
        hiveDb.getMSC().getNextNotification(eventId - 1, -1,
            new DatabaseAndTableFilter(work.dbNameToLoadIn, null)).getEvents();

    // Check the first eventId fetched is the same as what we fed, to ensure the events post that hasn't expired.
    if (notificationEvents.get(0).getEventId() != eventId) {
      throw new Exception("Failover notification events expired.");
    }
    // Remove the first one, it is already loaded, we fetched it to confirm the notification events post that haven't
    // expired.
    notificationEvents.remove(0);
    return notificationEvents;
  }

  /**
   * Prepares the table diff file, with tables modified post the specified event id.
   * @param notificationEvents Events that can possibly contain table DDL/DML metadata.
   * @param hiveDb the hive object
   * @param work the load work
   * @param conf hive configuration
   * @throws Exception
   */
  public static void prepareTableDiffFile(List<NotificationEvent> notificationEvents, Hive hiveDb,
                                          ReplLoadWork work, HiveConf conf) throws Exception {
    HashSet<String> modifiedTables = new HashSet<>();
    for (NotificationEvent event : notificationEvents) {
      String tableName = event.getTableName();
      if (tableName != null) {
        LOG.debug("Added table {} because of eventId {} and eventType {}", event.getTableName(), event.getEventId(),
            event.getEventType());
        modifiedTables.add(event.getTableName());
      }
    }
    Path dumpPath = new Path(work.dumpDirectory).getParent();
    FileSystem fs = dumpPath.getFileSystem(conf);
    Path diffFilePath = new Path(dumpPath, TABLE_DIFF_INPROGRESS_DIRECTORY);
    fs.mkdirs(diffFilePath);
    for (String table : modifiedTables) {
      String tables = "";
      LOG.info("Added table {} to table diff", table);
      ArrayList<String> pathList = getListing(work.dbNameToLoadIn, table, hiveDb, conf);
      for (String path : pathList) {
        tables += path + System.lineSeparator();
      }
      Utils.writeOutput(tables, new Path(diffFilePath, table), conf);
    }
    LOG.info("Completed writing table diff progress file at {} with entries {}", dumpPath, modifiedTables);
    // The operation is complete, we can rename to TABLE_DIFF_COMPLETE
    fs.rename(diffFilePath, new Path(dumpPath, TABLE_DIFF_COMPLETE_DIRECTORY));
    LOG.info("Completed renaming table diff progress file to table diff complete file.");
  }

  /**
   * Fetches the notification id from the database with respect to target database.
   * @param dbName name of database
   * @param hiveDb the hive object
   * @return the corresponding notification event id from target database
   * @throws Exception
   */
  public static String getTargetEventId(String dbName, Hive hiveDb) throws Exception {
    Database database = hiveDb.getDatabase(dbName);
    String targetLastEventId = getTargetLastReplicatedStateFromParameters(database.getParameters());
    List<NotificationEvent> events =
        hiveDb.getMSC().getNextNotification(Long.parseLong(targetLastEventId) - 1, 1, null).getEvents();
    if (events == null || events.isEmpty() || events.get(0).getEventId() != Long.parseLong(targetLastEventId)) {
      throw new IllegalStateException("Notification events are missing in the meta store.");
    }
    return targetLastEventId;
  }

  /**
   * Creates the bootstrap table list for the second load of optimised bootstrap to consume.
   * @param newDumpPath the dump path
   * @param tablesForBootstrap the list of tables.
   * @param conf the hive configuration
   * @throws SemanticException in case of any exception
   */
  public static void createBootstrapTableList(Path newDumpPath, Set<String> tablesForBootstrap,
      HiveConf conf) throws SemanticException {
    String tableList = "";
    for (String table : tablesForBootstrap) {
      tableList += table + System.lineSeparator();
    }
    LOG.info("Generated table list for optimised bootstrap {}", tableList);
    Utils.writeOutput(tableList, new Path(newDumpPath, BOOTSTRAP_TABLES_LIST), conf);
  }

  /**
   * Gets the list of tables for the second load of optimised bootstrap from the BOOTSTRAP_TABLE_LIST in the dump
   * directory.
   * @param dumpPath the dump path
   * @param conf the hive configuration
   * @return the array with list of tables to bootstrap
   * @throws IOException in case of any exception.
   */
  public static String[] getBootstrapTableList(Path dumpPath, HiveConf conf) throws IOException {
    Path tablePath = new Path(dumpPath, BOOTSTRAP_TABLES_LIST);
    String tableList = "";
    FileSystem fs = dumpPath.getFileSystem(conf);
    try (FSDataInputStream stream = fs.open(tablePath);) {
      tableList = IOUtils.toString(stream, Charset.defaultCharset());
    }
    return tableList.split(System.lineSeparator());
  }

  private static ArrayList<String> getListing(String dbName, String tableName, Hive hiveDb, HiveConf conf)
      throws HiveException, IOException {
    ArrayList<String> paths = new ArrayList<>();
    Table table = hiveDb.getTable(dbName, tableName, false);
    if (table == null) {
      LOG.info("Table {} not found, excluding the dropped table", tableName);
      // If the table is not there, return an empty list of paths, we would need to copy the entire stuff.
      return new ArrayList<>();
    }
    Path tableLocation = new Path(table.getSd().getLocation());
    paths.add(table.getSd().getLocation());
    FileSystem tableFs = tableLocation.getFileSystem(conf);
    buildListingForDirectory(paths, tableLocation, tableFs);
    // Check if the table is partitioned, in case the table is partitioned we need to check for the partitions
    // listing as well.
    if (table.isPartitioned()) {
      List<Partition> partitions = hiveDb.getPartitions(table);
      for (Partition part : partitions) {
        Path partPath = part.getDataLocation();
        // Build listing for the partition only if it doesn't lies within the table location, else it would have been
        // already included as part of recursive listing of table directory.
        if (!FileUtils.isPathWithinSubtree(partPath, tableLocation)) {
          buildListingForDirectory(paths, partPath, tableFs);
        }
      }
    }
    return paths;
  }

  private static void buildListingForDirectory(ArrayList<String> listing, Path tableLocation, FileSystem tableFs)
      throws IOException {
    if (!tableFs.exists(tableLocation)) {
      return;
    }
    RemoteIterator<FileStatus> itr = tableFs.listStatusIterator(tableLocation);
    while (itr.hasNext()) {
      FileStatus fstatus = itr.next();
      if (fstatus.isDirectory()) {
        listing.add(fstatus.getPath().toString());
        buildListingForDirectory(listing, fstatus.getPath(), tableFs);
      } else {
        listing.add(fstatus.getPath() + FILE_ENTRY_SEPARATOR + fstatus.getLen() + FILE_ENTRY_SEPARATOR + tableFs
            .getFileChecksum(fstatus.getPath()));
      }
    }
  }
}
