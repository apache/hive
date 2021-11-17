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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.event.filters.DatabaseAndTableFilter;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
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

import static org.apache.hadoop.hive.ql.parse.ReplicationSpec.getLastReplicatedStateFromParameters;

public class OptimisedBootstrapUtils {

  public static final String TABLE_SEPERATOR = "$#$#$#";
  public static final String FILE_ENTRY_SEPERATOR = "###";
  private static Logger LOG = LoggerFactory.getLogger(OptimisedBootstrapUtils.class);

  /**   table diff file when in progress */
  public static final String TABLE_DIFF_INPROGRESS_FILE = "table_diff";

  /** table diff file when complete */
  public static final String TABLE_DIFF_COMPLETE_FILE = "table_diff_complete";

  /** event ack file which contains the event id till which the cluster was last loaded. */
  public static final String EVENT_ACK_FILE = "event_ack";

  /**
   * Gets & checks whether the database is target of replication.
   * @param dbName name of database
   * @param hive hive object
   * @return true, if the database has repl.target.for property set.
   * @throws HiveException
   */
  public static boolean isFailover(String dbName, Hive hive) throws HiveException {
    Database database = hive.getDatabase(dbName);
    return database != null ? MetaStoreUtils.isTargetOfReplication(database) : false;
  }

  public static boolean checkFileExists(Path dumpPath, HiveConf conf, String fileName) throws IOException {
    FileSystem fs = dumpPath.getFileSystem(conf);
    return fs.exists(new Path(dumpPath, fileName));
  }

  /**
   * Gets the event id from the event ack file
   * @param dumpPath the dump path
   * @param conf the hive configuration
   * @return the event id from file.
   * @throws IOException
   */
  public static String getEventIdFromFile(Path dumpPath, HiveConf conf) throws IOException {
    String lastEventId;
    Path eventAckFilePath = new Path(dumpPath, EVENT_ACK_FILE);
    FileSystem fs = eventAckFilePath.getFileSystem(conf);
    try (FSDataInputStream stream = fs.open(eventAckFilePath);) {
      lastEventId = IOUtils.toString(stream, Charset.defaultCharset());
    }
    return lastEventId.replaceAll(System.lineSeparator(),"").trim();
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
    Path tableDiffPath = new Path(dumpPath, TABLE_DIFF_COMPLETE_FILE);
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
    Path tableDiffPath = new Path(dumpPath, TABLE_DIFF_COMPLETE_FILE);
    Path filePath = new Path(tableDiffPath, file);
    String allEntries;
    try (FSDataInputStream stream = fs.open(filePath);) {
      allEntries = IOUtils.toString(stream, Charset.defaultCharset());
    }
    paths.addAll(Arrays.asList(allEntries.split(System.lineSeparator())));
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
   * Creates the event ack file and sets the dump metadata post that marking completion of dump flow for first round
   * of optimised failover dump.
   * @param currentDumpPath the dump path
   * @param dmd the dump metadata
   * @param cmRoot the cmRoot
   * @param dbEventId the database event id to which we have to write in the file.
   * @param conf the hive configuraiton
   * @param work the repldump work
   * @return the lastReplId denoting a fake dump(-1) always
   * @throws SemanticException
   */
  public static Long getAndCreateEventAckFile(Path currentDumpPath, DumpMetaData dmd, Path cmRoot, String dbEventId,
      HiveConf conf, ReplDumpWork work)
      throws SemanticException {
    // Keep an invalid value for lastReplId, to denote it isn't a actual dump.
    Long lastReplId = -1L;
    Path filePath = new Path(currentDumpPath, EVENT_ACK_FILE);
    Utils.writeOutput(dbEventId, filePath, conf);
    LOG.info("Created event_ack file at {} with eventId {}", filePath, dbEventId);
    work.setResultValues(Arrays.asList(currentDumpPath.toUri().toString(), String.valueOf(lastReplId)));
    dmd.setDump(DumpType.INCREMENTAL, work.eventFrom, lastReplId, cmRoot, -1L, false);
    dmd.write(true);
    return lastReplId;
  }

  /**
   * Prepares the table diff file, with tables modified post the specified event id.
   * @param eventId the event id after which tables should be modified
   * @param hiveDb the hive object
   * @param work the load work
   * @param conf hive configuration
   * @throws Exception
   */
  public static void prepareTableDiffFile(Long eventId, Hive hiveDb, ReplLoadWork work, HiveConf conf)
      throws Exception {
    // Create a table diff file.
    List<NotificationEvent> notificationEvents =
        hiveDb.getMSC().getNextNotification(eventId - 1, -1, new DatabaseAndTableFilter(work.dbNameToLoadIn, null))
            .getEvents();
    // Check the first eventId fetched is the same as what we fed, to ensure the events post that hasn't expired.
    if (notificationEvents.get(0).getEventId() != eventId) {
      throw new Exception("Failover notification events expired.");
    }
    notificationEvents.remove(0);
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
    Path diffFilePath = new Path(dumpPath, TABLE_DIFF_INPROGRESS_FILE);
    fs.mkdirs(diffFilePath);
    for (String table : modifiedTables) {
      String tables = "";
      LOG.info("Added table {} to table diff", table);
      ArrayList<String> pathList = getListing(work.dbNameToLoadIn, table, hiveDb, conf);
      tables += table + System.lineSeparator();
      for (String path : pathList) {
        tables += path + System.lineSeparator();
      }
      Utils.writeOutput(tables, new Path(diffFilePath, table), conf);
    }
    LOG.info("Completed writing table diff progress file at {} with entries {}", dumpPath, modifiedTables);
    // The operation is complete, we can rename to TABLE_DIFF_COMPLETE
    fs.rename(diffFilePath, new Path(dumpPath, TABLE_DIFF_COMPLETE_FILE));
    LOG.info("Completed renaming table diff progress file to table diff complete file.");
  }

  private static ArrayList<String> getListing(String dbNameToLoadIn, String tableName, Hive hiveDb, HiveConf conf)
      throws HiveException, IOException {
    ArrayList<String> paths = new ArrayList<>();
    Table table = hiveDb.getTable(dbNameToLoadIn, tableName);
    if (table == null) {
      return new ArrayList<>();
    }
    Path tableLocation = new Path(table.getSd().getLocation());
    paths.add(table.getSd().getLocation());
    FileSystem tableFs = tableLocation.getFileSystem(conf);
    buildListingForDirectory(paths, tableLocation, tableFs);
    if (table.isPartitioned()) {
      List<Partition> partitions = hiveDb.getPartitions(table);
      for (Partition part : partitions) {
        Path partPath = part.getDataLocation();
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
        listing.add(fstatus.getPath() + FILE_ENTRY_SEPERATOR + fstatus.getLen() + "###" + tableFs.getFileChecksum(fstatus.getPath()));
      }
    }
  }
}
