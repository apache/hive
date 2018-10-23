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
package org.apache.hive.hcatalog.listener;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreEventListenerConstants;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RawStoreProxy;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.AddForeignKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddNotNullConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AddPrimaryKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddUniqueConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropConstraintEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.messaging.AcidWriteMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.PartitionFiles;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.DatabaseProduct.MYSQL;

/**
 * An implementation of {@link org.apache.hadoop.hive.metastore.MetaStoreEventListener} that
 * stores events in the database.
 *
 * Design overview:  This listener takes any event, builds a NotificationEventResponse,
 * and puts it on a queue.  There is a dedicated thread that reads entries from the queue and
 * places them in the database.  The reason for doing it in a separate thread is that we want to
 * avoid slowing down other metadata operations with the work of putting the notification into
 * the database.  Also, occasionally the thread needs to clean the database of old records.  We
 * definitely don't want to do that as part of another metadata operation.
 */
public class DbNotificationListener extends TransactionalMetaStoreEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(DbNotificationListener.class.getName());
  private static CleanerThread cleaner = null;

  private Configuration conf;
  private MessageFactory msgFactory;

  //cleaner is a static object, use static synchronized to make sure its thread-safe
  private static synchronized void init(Configuration conf) throws MetaException {
    if (cleaner == null) {
      cleaner =
          new CleanerThread(conf, RawStoreProxy.getProxy(conf, conf,
              MetastoreConf.getVar(conf, ConfVars.RAW_STORE_IMPL), 999999));
      cleaner.start();
    }
  }

  public DbNotificationListener(Configuration config) throws MetaException {
    super(config);
    conf = config;
    DbNotificationListener.init(conf);
    msgFactory = MessageFactory.getInstance();
  }

  /**
   * @param tableEvent table event.
   * @throws org.apache.hadoop.hive.metastore.api.MetaException
   */
  @Override
  public void onConfigChange(ConfigChangeEvent tableEvent) throws MetaException {
    String key = tableEvent.getKey();
    if (key.equals(ConfVars.EVENT_DB_LISTENER_TTL.toString()) ||
        key.equals(ConfVars.EVENT_DB_LISTENER_TTL.getHiveName())) {
      // This weirdness of setting it in our conf and then reading back does two things.
      // One, it handles the conversion of the TimeUnit.  Two, it keeps the value around for
      // later in case we need it again.
      long time = MetastoreConf.convertTimeStr(tableEvent.getNewValue(), TimeUnit.SECONDS,
          TimeUnit.SECONDS);
      MetastoreConf.setTimeVar(getConf(), MetastoreConf.ConfVars.EVENT_DB_LISTENER_TTL, time,
          TimeUnit.SECONDS);
      cleaner.setTimeToLive(MetastoreConf.getTimeVar(getConf(),
          MetastoreConf.ConfVars.EVENT_DB_LISTENER_TTL, TimeUnit.SECONDS));
    }

    if (key.equals(ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL.toString()) ||
            key.equals(ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL.getHiveName())) {
      // This weirdness of setting it in our conf and then reading back does two things.
      // One, it handles the conversion of the TimeUnit.  Two, it keeps the value around for
      // later in case we need it again.
      long time = MetastoreConf.convertTimeStr(tableEvent.getNewValue(), TimeUnit.SECONDS,
              TimeUnit.SECONDS);
      MetastoreConf.setTimeVar(getConf(), MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL, time,
              TimeUnit.SECONDS);
      cleaner.setCleanupInterval(MetastoreConf.getTimeVar(getConf(),
              MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL, TimeUnit.MILLISECONDS));
    }
  }

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    Table t = tableEvent.getTable();
    FileIterator fileIter = MetaStoreUtils.isExternalTable(t)
                              ? null : new FileIterator(t.getSd().getLocation());
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.CREATE_TABLE.toString(),
                msgFactory.buildCreateTableMessage(t, fileIter).toString());
    event.setCatName(t.isSetCatName() ? t.getCatName() : DEFAULT_CATALOG_NAME);
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    process(event, tableEvent);
  }

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    Table t = tableEvent.getTable();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_TABLE.toString(), msgFactory
            .buildDropTableMessage(t).toString());
    event.setCatName(t.isSetCatName() ? t.getCatName() : DEFAULT_CATALOG_NAME);
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    process(event, tableEvent);
  }

  /**
   * @param tableEvent alter table event
   * @throws MetaException
   */
  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
    Table before = tableEvent.getOldTable();
    Table after = tableEvent.getNewTable();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ALTER_TABLE.toString(), msgFactory
            .buildAlterTableMessage(before, after, tableEvent.getIsTruncateOp(), tableEvent.getWriteId()).toString());
    event.setCatName(after.isSetCatName() ? after.getCatName() : DEFAULT_CATALOG_NAME);
    event.setDbName(after.getDbName());
    event.setTableName(after.getTableName());
    process(event, tableEvent);
  }

  class FileIterator implements Iterator<String> {
    /***
     * Filter for valid files only (no dir, no hidden)
     */
    PathFilter VALID_FILES_FILTER = new PathFilter() {
      @Override
      public boolean accept(Path p) {
        try {
          if (!fs.isFile(p)) {
            return false;
          }
          String name = p.getName();
          return !name.startsWith("_") && !name.startsWith(".");
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    private FileSystem fs;
    private FileStatus[] files;
    private int i = 0;
    FileIterator(String locString) {
      try {
        if (locString != null) {
          Path loc = new Path(locString);
          fs = loc.getFileSystem(conf);
          files = fs.listStatus(loc, VALID_FILES_FILTER);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean hasNext() {
      if (files == null) {
        return false;
      }
      return i<files.length;
    }

    @Override
    public String next() {
      try {
        FileStatus file = files[i];
        i++;
        return ReplChangeManager.encodeFileUri(file.getPath().toString(),
            ReplChangeManager.checksumFor(file.getPath(), fs), null);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  class PartitionFilesIterator implements Iterator<PartitionFiles> {

    private Iterator<Partition> partitionIter;
    private Table t;

    PartitionFilesIterator(Iterator<Partition> partitionIter, Table t) {
      this.partitionIter = partitionIter;
      this.t = t;
    }
    @Override
    public boolean hasNext() {
      return partitionIter.hasNext();
    }

    @Override
    public PartitionFiles next() {
      try {
        Partition p = partitionIter.next();
        Iterator<String> fileIterator;
        //For transactional tables, the actual file copy will be done by acid write event during replay of commit txn.
        if (!TxnUtils.isTransactionalTable(t)) {
          List<String> files = Lists.newArrayList(new FileIterator(p.getSd().getLocation()));
          fileIterator = files.iterator();
        } else {
          fileIterator = Collections.emptyIterator();
        }
        PartitionFiles partitionFiles =
            new PartitionFiles(Warehouse.makePartName(t.getPartitionKeys(), p.getValues()), fileIterator);
        return partitionFiles;
      } catch (MetaException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    Table t = partitionEvent.getTable();
    PartitionFilesIterator fileIter = MetaStoreUtils.isExternalTable(t)
            ? null : new PartitionFilesIterator(partitionEvent.getPartitionIterator(), t);
    String msg = msgFactory
        .buildAddPartitionMessage(t, partitionEvent.getPartitionIterator(), fileIter).toString();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ADD_PARTITION.toString(), msg);
    event.setCatName(t.isSetCatName() ? t.getCatName() : DEFAULT_CATALOG_NAME);
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    process(event, partitionEvent);
  }

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    Table t = partitionEvent.getTable();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_PARTITION.toString(), msgFactory
            .buildDropPartitionMessage(t, partitionEvent.getPartitionIterator()).toString());
    event.setCatName(t.isSetCatName() ? t.getCatName() : DEFAULT_CATALOG_NAME);
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    process(event, partitionEvent);
  }

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
    Partition before = partitionEvent.getOldPartition();
    Partition after = partitionEvent.getNewPartition();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ALTER_PARTITION.toString(), msgFactory
            .buildAlterPartitionMessage(partitionEvent.getTable(), before, after, partitionEvent.getIsTruncateOp(),
                    partitionEvent.getWriteId()).toString());
    event.setCatName(before.isSetCatName() ? before.getCatName() : DEFAULT_CATALOG_NAME);
    event.setDbName(before.getDbName());
    event.setTableName(before.getTableName());
    process(event, partitionEvent);
  }

  /**
   * @param dbEvent database event
   * @throws MetaException
   */
  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
    Database db = dbEvent.getDatabase();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.CREATE_DATABASE.toString(), msgFactory
            .buildCreateDatabaseMessage(db).toString());
    event.setCatName(db.isSetCatalogName() ? db.getCatalogName() : DEFAULT_CATALOG_NAME);
    event.setDbName(db.getName());
    process(event, dbEvent);
  }

  /**
   * @param dbEvent database event
   * @throws MetaException
   */
  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    Database db = dbEvent.getDatabase();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_DATABASE.toString(), msgFactory
            .buildDropDatabaseMessage(db).toString());
    event.setCatName(db.isSetCatalogName() ? db.getCatalogName() : DEFAULT_CATALOG_NAME);
    event.setDbName(db.getName());
    process(event, dbEvent);
  }

  /**
   * @param dbEvent alter database event
   * @throws MetaException
   */
  @Override
  public void onAlterDatabase(AlterDatabaseEvent dbEvent) throws MetaException {
    Database oldDb = dbEvent.getOldDatabase();
    Database newDb = dbEvent.getNewDatabase();
    NotificationEvent event =
            new NotificationEvent(0, now(), EventType.ALTER_DATABASE.toString(), msgFactory
                    .buildAlterDatabaseMessage(oldDb, newDb).toString());
    event.setCatName(oldDb.isSetCatalogName() ? oldDb.getCatalogName() : DEFAULT_CATALOG_NAME);
    event.setDbName(oldDb.getName());
    process(event, dbEvent);
  }

  /**
   * @param fnEvent function event
   * @throws MetaException
   */
  @Override
  public void onCreateFunction(CreateFunctionEvent fnEvent) throws MetaException {
    Function fn = fnEvent.getFunction();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.CREATE_FUNCTION.toString(), msgFactory
            .buildCreateFunctionMessage(fn).toString());
    event.setCatName(fn.isSetCatName() ? fn.getCatName() : DEFAULT_CATALOG_NAME);
    event.setDbName(fn.getDbName());
    process(event, fnEvent);
  }

  /**
   * @param fnEvent function event
   * @throws MetaException
   */
  @Override
  public void onDropFunction(DropFunctionEvent fnEvent) throws MetaException {
    Function fn = fnEvent.getFunction();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_FUNCTION.toString(), msgFactory
            .buildDropFunctionMessage(fn).toString());
    event.setCatName(fn.isSetCatName() ? fn.getCatName() : DEFAULT_CATALOG_NAME);
    event.setDbName(fn.getDbName());
    process(event, fnEvent);
  }

  class FileChksumIterator implements Iterator<String> {
    private List<String> files;
    private List<String> chksums;
    private List<String> subDirs;
    int i = 0;
    FileChksumIterator(List<String> files, List<String> chksums) {
      this(files, chksums, null);
    }
    FileChksumIterator(List<String> files, List<String> chksums, List<String> subDirs) {
      this.files = files;
      this.chksums = chksums;
      this.subDirs = subDirs;
    }
    @Override
    public boolean hasNext() {
      return i< files.size();
    }

    @Override
    public String next() {
      String result;
      try {
        result = ReplChangeManager.encodeFileUri(files.get(i), chksums != null ? chksums.get(i) : null,
                subDirs != null ? subDirs.get(i) : null);
      } catch (IOException e) {
        // File operations failed
        LOG.error("Encoding file URI failed with error " + e.getMessage());
        throw new RuntimeException(e.getMessage());
      }
      i++;
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  @Override
  public void onInsert(InsertEvent insertEvent) throws MetaException {
    Table tableObj = insertEvent.getTableObj();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.INSERT.toString(), msgFactory.buildInsertMessage(tableObj,
                insertEvent.getPartitionObj(), insertEvent.isReplace(),
            new FileChksumIterator(insertEvent.getFiles(), insertEvent.getFileChecksums()))
                .toString());
    event.setCatName(tableObj.isSetCatName() ? tableObj.getCatName() : DEFAULT_CATALOG_NAME);
    event.setDbName(tableObj.getDbName());
    event.setTableName(tableObj.getTableName());
    process(event, insertEvent);
  }

  @Override
  public void onOpenTxn(OpenTxnEvent openTxnEvent, Connection dbConn, SQLGenerator sqlGenerator) throws MetaException {
    int lastTxnIdx = openTxnEvent.getTxnIds().size() - 1;
    OpenTxnMessage msg = msgFactory.buildOpenTxnMessage(openTxnEvent.getTxnIds().get(0),
            openTxnEvent.getTxnIds().get(lastTxnIdx));
    NotificationEvent event =
            new NotificationEvent(0, now(), EventType.OPEN_TXN.toString(), msg.toString());

    try {
      addNotificationLog(event, openTxnEvent, dbConn, sqlGenerator);
    } catch (SQLException e) {
      throw new MetaException("Unable to execute direct SQL " + StringUtils.stringifyException(e));
    }
  }

  @Override
  public void onCommitTxn(CommitTxnEvent commitTxnEvent, Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
    NotificationEvent event =
            new NotificationEvent(0, now(), EventType.COMMIT_TXN.toString(), msgFactory.buildCommitTxnMessage(
                    commitTxnEvent.getTxnId())
                    .toString());

    try {
      addNotificationLog(event, commitTxnEvent, dbConn, sqlGenerator);
    } catch (SQLException e) {
      throw new MetaException("Unable to execute direct SQL " + StringUtils.stringifyException(e));
    }
  }

  @Override
  public void onAbortTxn(AbortTxnEvent abortTxnEvent, Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ABORT_TXN.toString(), msgFactory.buildAbortTxnMessage(
            abortTxnEvent.getTxnId())
            .toString());

    try {
      addNotificationLog(event, abortTxnEvent, dbConn, sqlGenerator);
    } catch (SQLException e) {
      throw new MetaException("Unable to execute direct SQL " + StringUtils.stringifyException(e));
    }
  }

  /**
   * @param partSetDoneEvent
   * @throws MetaException
   */
  @Override
  public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {
    // TODO, we don't support this, but we should, since users may create an empty partition and
    // then load data into it.
  }

  /***
   * @param addPrimaryKeyEvent add primary key event
   * @throws MetaException
   */
  @Override
  public void onAddPrimaryKey(AddPrimaryKeyEvent addPrimaryKeyEvent) throws MetaException {
    List<SQLPrimaryKey> cols = addPrimaryKeyEvent.getPrimaryKeyCols();
    if (cols.size() > 0) {
      NotificationEvent event =
          new NotificationEvent(0, now(), EventType.ADD_PRIMARYKEY.toString(), msgFactory
              .buildAddPrimaryKeyMessage(addPrimaryKeyEvent.getPrimaryKeyCols()).toString());
      event.setCatName(cols.get(0).isSetCatName() ? cols.get(0).getCatName() : DEFAULT_CATALOG_NAME);
      event.setDbName(cols.get(0).getTable_db());
      event.setTableName(cols.get(0).getTable_name());
      process(event, addPrimaryKeyEvent);
    }
  }

  /***
   * @param addForeignKeyEvent add foreign key event
   * @throws MetaException
   */
  @Override
  public void onAddForeignKey(AddForeignKeyEvent addForeignKeyEvent) throws MetaException {
    List<SQLForeignKey> cols = addForeignKeyEvent.getForeignKeyCols();
    if (cols.size() > 0) {
      NotificationEvent event =
          new NotificationEvent(0, now(), EventType.ADD_FOREIGNKEY.toString(), msgFactory
              .buildAddForeignKeyMessage(addForeignKeyEvent.getForeignKeyCols()).toString());
      event.setCatName(cols.get(0).isSetCatName() ? cols.get(0).getCatName() : DEFAULT_CATALOG_NAME);
      event.setDbName(cols.get(0).getPktable_db());
      event.setTableName(cols.get(0).getPktable_name());
      process(event, addForeignKeyEvent);
    }
  }

  /***
   * @param addUniqueConstraintEvent add unique constraint event
   * @throws MetaException
   */
  @Override
  public void onAddUniqueConstraint(AddUniqueConstraintEvent addUniqueConstraintEvent) throws MetaException {
    List<SQLUniqueConstraint> cols = addUniqueConstraintEvent.getUniqueConstraintCols();
    if (cols.size() > 0) {
      NotificationEvent event =
          new NotificationEvent(0, now(), EventType.ADD_UNIQUECONSTRAINT.toString(), msgFactory
              .buildAddUniqueConstraintMessage(addUniqueConstraintEvent.getUniqueConstraintCols()).toString());
      event.setCatName(cols.get(0).isSetCatName() ? cols.get(0).getCatName() : DEFAULT_CATALOG_NAME);
      event.setDbName(cols.get(0).getTable_db());
      event.setTableName(cols.get(0).getTable_name());
      process(event, addUniqueConstraintEvent);
    }
  }

  /***
   * @param addNotNullConstraintEvent add not null constraint event
   * @throws MetaException
   */
  @Override
  public void onAddNotNullConstraint(AddNotNullConstraintEvent addNotNullConstraintEvent) throws MetaException {
    List<SQLNotNullConstraint> cols = addNotNullConstraintEvent.getNotNullConstraintCols();
    if (cols.size() > 0) {
      NotificationEvent event =
          new NotificationEvent(0, now(), EventType.ADD_NOTNULLCONSTRAINT.toString(), msgFactory
              .buildAddNotNullConstraintMessage(addNotNullConstraintEvent.getNotNullConstraintCols()).toString());
      event.setCatName(cols.get(0).isSetCatName() ? cols.get(0).getCatName() : DEFAULT_CATALOG_NAME);
      event.setDbName(cols.get(0).getTable_db());
      event.setTableName(cols.get(0).getTable_name());
      process(event, addNotNullConstraintEvent);
    }
  }

  /***
   * @param dropConstraintEvent drop constraint event
   * @throws MetaException
   */
  @Override
  public void onDropConstraint(DropConstraintEvent dropConstraintEvent) throws MetaException {
    String dbName = dropConstraintEvent.getDbName();
    String tableName = dropConstraintEvent.getTableName();
    String constraintName = dropConstraintEvent.getConstraintName();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_CONSTRAINT.toString(), msgFactory
            .buildDropConstraintMessage(dbName, tableName, constraintName).toString());
    event.setCatName(dropConstraintEvent.getCatName());
    event.setDbName(dbName);
    event.setTableName(tableName);
    process(event, dropConstraintEvent);
  }

  /***
   * @param allocWriteIdEvent Alloc write id event
   * @throws MetaException
   */
  @Override
  public void onAllocWriteId(AllocWriteIdEvent allocWriteIdEvent, Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
    String tableName = allocWriteIdEvent.getTableName();
    String dbName = allocWriteIdEvent.getDbName();
    NotificationEvent event =
            new NotificationEvent(0, now(), EventType.ALLOC_WRITE_ID.toString(), msgFactory
                    .buildAllocWriteIdMessage(allocWriteIdEvent.getTxnToWriteIdList(), dbName, tableName).toString());
    event.setTableName(tableName);
    event.setDbName(dbName);
    try {
      addNotificationLog(event, allocWriteIdEvent, dbConn, sqlGenerator);
    } catch (SQLException e) {
      throw new MetaException("Unable to execute direct SQL " + StringUtils.stringifyException(e));
    }
  }

  @Override
  public void onAcidWrite(AcidWriteEvent acidWriteEvent, Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
    AcidWriteMessage msg = msgFactory.buildAcidWriteMessage(acidWriteEvent,
            new FileChksumIterator(acidWriteEvent.getFiles(), acidWriteEvent.getChecksums(),
                    acidWriteEvent.getSubDirs()));
    NotificationEvent event = new NotificationEvent(0, now(), EventType.ACID_WRITE.toString(), msg.toString());
    event.setMessageFormat(msgFactory.getMessageFormat());
    event.setDbName(acidWriteEvent.getDatabase());
    event.setTableName(acidWriteEvent.getTable());
    try {
      addWriteNotificationLog(event, acidWriteEvent, dbConn, sqlGenerator, msg);
    } catch (SQLException e) {
      throw new MetaException("Unable to add write notification log " + StringUtils.stringifyException(e));
    }
  }

  private int now() {
    long millis = System.currentTimeMillis();
    millis /= 1000;
    if (millis > Integer.MAX_VALUE) {
      LOG.warn("We've passed max int value in seconds since the epoch, " +
          "all notification times will be the same!");
      return Integer.MAX_VALUE;
    }
    return (int)millis;
  }

  /**
   * Close statement instance.
   * @param stmt statement instance.
   */
  private static void closeStmt(Statement stmt) {
    try {
      if (stmt != null && !stmt.isClosed()) {
        stmt.close();
      }
    } catch (SQLException e) {
      LOG.warn("Failed to close statement " + e.getMessage());
    }
  }

  /**
   * Close the ResultSet.
   * @param rs may be {@code null}
   */
  private static void close(ResultSet rs) {
    try {
      if (rs != null && !rs.isClosed()) {
        rs.close();
      }
    } catch(SQLException ex) {
      LOG.warn("Failed to close result set " + ex.getMessage());
    }
  }

  private long getNextNLId(Statement stmt, SQLGenerator sqlGenerator, String sequence)
          throws SQLException, MetaException {
    String s = sqlGenerator.addForUpdateClause("select \"NEXT_VAL\" from " +
            "\"SEQUENCE_TABLE\" where \"SEQUENCE_NAME\" = " + quoteString(sequence));
    LOG.debug("Going to execute query <" + s + ">");
    ResultSet rs = null;
    try {
      rs = stmt.executeQuery(s);
      if (!rs.next()) {
        throw new MetaException("Transaction database not properly configured, can't find next NL id.");
      }

      long nextNLId = rs.getLong(1);
      long updatedNLId = nextNLId + 1;
      s = "update \"SEQUENCE_TABLE\" set \"NEXT_VAL\" = " + updatedNLId + " where \"SEQUENCE_NAME\" = " +
              quoteString(sequence);
      LOG.debug("Going to execute update <" + s + ">");
      stmt.executeUpdate(s);
      return nextNLId;
    }finally {
      close(rs);
    }
  }

  private void addWriteNotificationLog(NotificationEvent event, AcidWriteEvent acidWriteEvent, Connection dbConn,
                                 SQLGenerator sqlGenerator, AcidWriteMessage msg) throws MetaException, SQLException {
    LOG.debug("DbNotificationListener: adding write notification log for : {}", event.getMessage());
    assert ((dbConn != null) && (sqlGenerator != null));

    Statement stmt = null;
    PreparedStatement pst = null;
    ResultSet rs = null;
    String dbName = acidWriteEvent.getDatabase();
    String tblName = acidWriteEvent.getTable();
    String partition = acidWriteEvent.getPartition();
    String tableObj = msg.getTableObjStr();
    String partitionObj = msg.getPartitionObjStr();
    String files = ReplChangeManager.joinWithSeparator(msg.getFiles());

    try {
      stmt = dbConn.createStatement();
      if (sqlGenerator.getDbProduct() == MYSQL) {
        stmt.execute("SET @@session.sql_mode=ANSI_QUOTES");
      }

      String s = sqlGenerator.addForUpdateClause("select \"WNL_FILES\", \"WNL_ID\" from" +
                      " \"TXN_WRITE_NOTIFICATION_LOG\" " +
                      "where \"WNL_DATABASE\" = ? " +
                      "and \"WNL_TABLE\" = ? " +  " and \"WNL_PARTITION\" = ? " +
                      "and \"WNL_TXNID\" = " + Long.toString(acidWriteEvent.getTxnId()));
      List<String> params = Arrays.asList(dbName, tblName, partition);
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
      LOG.debug("Going to execute query <" + s.replaceAll("\\?", "{}") + ">",
              quoteString(dbName), quoteString(tblName), quoteString(partition));
      rs = pst.executeQuery();
      if (!rs.next()) {
        // if rs is empty then no lock is taken and thus it can not cause deadlock.
        long nextNLId = getNextNLId(stmt, sqlGenerator,
                "org.apache.hadoop.hive.metastore.model.MTxnWriteNotificationLog");
        s = "insert into \"TXN_WRITE_NOTIFICATION_LOG\" " +
                "(\"WNL_ID\", \"WNL_TXNID\", \"WNL_WRITEID\", \"WNL_DATABASE\", \"WNL_TABLE\", " +
                "\"WNL_PARTITION\", \"WNL_TABLE_OBJ\", \"WNL_PARTITION_OBJ\", " +
                "\"WNL_FILES\", \"WNL_EVENT_TIME\") VALUES (?,?,?,?,?,?,?,?,?,?)";
        closeStmt(pst);
        pst = dbConn.prepareStatement(sqlGenerator.addEscapeCharacters(s));
        pst.setLong(1, nextNLId);
        pst.setLong(2, acidWriteEvent.getTxnId());
        pst.setLong(3, acidWriteEvent.getWriteId());
        pst.setString(4, dbName);
        pst.setString(5, tblName);
        pst.setString(6, partition);
        pst.setString(7, tableObj);
        pst.setString(8, partitionObj);
        pst.setString(9, files);
        pst.setInt(10, now());
        LOG.debug("Going to execute insert <" + s.replaceAll("\\?", "{}") + ">", nextNLId,
                acidWriteEvent.getTxnId(), acidWriteEvent.getWriteId(), quoteString(dbName), quoteString(tblName),
                quoteString(partition), quoteString(tableObj), quoteString(partitionObj), quoteString(files), now());
        pst.execute();
      } else {
        String existingFiles = rs.getString(1);
        if (existingFiles.contains(sqlGenerator.addEscapeCharacters(files))) {
          // If list of files are already present then no need to update it again. This scenario can come in case of
          // retry done to the meta store for the same operation.
          LOG.info("file list " + files + " already present");
          return;
        }
        long nlId = rs.getLong(2);
        files = ReplChangeManager.joinWithSeparator(Lists.newArrayList(files, existingFiles));
        s = "update \"TXN_WRITE_NOTIFICATION_LOG\" set \"WNL_TABLE_OBJ\" = ? ," +
                " \"WNL_PARTITION_OBJ\" = ? ," +
                " \"WNL_FILES\" = ? ," +
                " \"WNL_EVENT_TIME\" = ?" +
                " where \"WNL_ID\" = ?";
        closeStmt(pst);
        pst = dbConn.prepareStatement(sqlGenerator.addEscapeCharacters(s));
        pst.setString(1, tableObj);
        pst.setString(2, partitionObj);
        pst.setString(3, files);
        pst.setInt(4, now());
        pst.setLong(5, nlId);
        LOG.debug("Going to execute update <" + s.replaceAll("\\?", "{}") + ">", quoteString(tableObj),
                quoteString(partitionObj), quoteString(files), now(), nlId);
        pst.executeUpdate();
      }
    } catch (SQLException e) {
      LOG.warn("failed to add write notification log" + e.getMessage());
      // eat the exception for now, will uncomment if everything works
      // throw e;
    } finally {
      closeStmt(stmt);
      closeStmt(pst);
      close(rs);
    }
  }

  static String quoteString(String input) {
    return "'" + input + "'";
  }

  private void addNotificationLog(NotificationEvent event, ListenerEvent listenerEvent, Connection dbConn,
                                  SQLGenerator sqlGenerator) throws MetaException, SQLException {
    LOG.debug("DbNotificationListener: adding notification log for : {}", event.getMessage());
    if ((dbConn == null) || (sqlGenerator == null)) {
      LOG.info("connection or sql generator is not set so executing sql via DN");
      process(event, listenerEvent);
      return;
    }
    Statement stmt = null;
    PreparedStatement pst = null;
    ResultSet rs = null;
    try {
      stmt = dbConn.createStatement();
      event.setMessageFormat(msgFactory.getMessageFormat());

      if (sqlGenerator.getDbProduct() == MYSQL) {
        stmt.execute("SET @@session.sql_mode=ANSI_QUOTES");
      }

      String s = sqlGenerator.addForUpdateClause("select \"NEXT_EVENT_ID\" " +
              " from \"NOTIFICATION_SEQUENCE\"");
      LOG.debug("Going to execute query <" + s + ">");
      rs = stmt.executeQuery(s);
      if (!rs.next()) {
        throw new MetaException("Transaction database not properly " +
                "configured, can't find next event id.");
      }
      long nextEventId = rs.getLong(1);
      long updatedEventid = nextEventId + 1;
      s = "update \"NOTIFICATION_SEQUENCE\" set \"NEXT_EVENT_ID\" = " + updatedEventid;
      LOG.debug("Going to execute update <" + s + ">");
      stmt.executeUpdate(s);

      long nextNLId = getNextNLId(stmt, sqlGenerator,
              "org.apache.hadoop.hive.metastore.model.MNotificationLog");

      String insertVal;
      String columns;
      List<String> params = new ArrayList<String>();

      // Construct the values string, parameters and column string step by step simultaneously so
      // that the positions of columns and of their corresponding values do not go out of sync.

      // Notification log id
      columns = "\"NL_ID\"";
      insertVal = "" + nextNLId;

      // Event id
      columns = columns + ", \"EVENT_ID\"";
      insertVal = insertVal + "," + nextEventId;

      // Event time
      columns = columns + ", \"EVENT_TIME\"";
      insertVal = insertVal + "," + now();

      // Event type
      columns = columns + ", \"EVENT_TYPE\"";
      insertVal = insertVal + ", ?";
      params.add(event.getEventType());

      // Message
      columns = columns + ", \"MESSAGE\"";
      insertVal = insertVal + ", ?";
      params.add(event.getMessage());

      // Message format
      columns = columns + ", \"MESSAGE_FORMAT\"";
      insertVal = insertVal + ", ?";
      params.add(event.getMessageFormat());

      // Database name, optional
      String dbName = event.getDbName();
      if (dbName != null) {
        assert dbName.equals(dbName.toLowerCase());
        columns = columns + ", \"DB_NAME\"";
        insertVal = insertVal + ", ?";
        params.add(dbName);
      }

      // Table name, optional
      String tableName = event.getTableName();
      if (tableName != null) {
        assert tableName.equals(tableName.toLowerCase());
        columns = columns + ", \"TBL_NAME\"";
        insertVal = insertVal + ", ?";
        params.add(tableName);
      }

      // Catalog name, optional
      String catName = event.getCatName();
      if (catName != null) {
        assert catName.equals(catName.toLowerCase());
        columns = columns + ", \"CAT_NAME\"";
        insertVal = insertVal + ", ?";
        params.add(catName);
      }

      s = "insert into \"NOTIFICATION_LOG\" (" + columns + ") VALUES (" + insertVal + ")";
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
      LOG.debug("Going to execute insert <" + s + "> with parameters (" +
              String.join(", ", params) + ")");
      pst.execute();

      // Set the DB_NOTIFICATION_EVENT_ID for future reference by other listeners.
      if (event.isSetEventId()) {
        listenerEvent.putParameter(
                MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME,
                Long.toString(event.getEventId()));
      }
    } catch (SQLException e) {
      LOG.warn("failed to add notification log" + e.getMessage());
      throw e;
    } finally {
      closeStmt(stmt);
      closeStmt(pst);
      close(rs);
    }
  }

  /**
   * Process this notification by adding it to metastore DB.
   *
   * @param event NotificationEvent is the object written to the metastore DB.
   * @param listenerEvent ListenerEvent (from which NotificationEvent was based) used only to set the
   *                      DB_NOTIFICATION_EVENT_ID_KEY_NAME for future reference by other listeners.
   */
  private void process(NotificationEvent event, ListenerEvent listenerEvent) throws MetaException {
    event.setMessageFormat(msgFactory.getMessageFormat());
    LOG.debug("DbNotificationListener: Processing : {}:{}", event.getEventId(),
        event.getMessage());
    HMSHandler.getMSForConf(conf).addNotificationEvent(event);

    // Set the DB_NOTIFICATION_EVENT_ID for future reference by other listeners.
    if (event.isSetEventId()) {
      listenerEvent.putParameter(
          MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME,
          Long.toString(event.getEventId()));
    }
  }

  private static class CleanerThread extends Thread {
    private RawStore rs;
    private int ttl;
    private long sleepTime;

    CleanerThread(Configuration conf, RawStore rs) {
      super("DB-Notification-Cleaner");
      this.rs = rs;
      setTimeToLive(MetastoreConf.getTimeVar(conf, ConfVars.EVENT_DB_LISTENER_TTL,
          TimeUnit.SECONDS));
      setCleanupInterval(MetastoreConf.getTimeVar(conf, ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL,
              TimeUnit.MILLISECONDS));
      setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        try {
          rs.cleanNotificationEvents(ttl);
          rs.cleanWriteNotificationEvents(ttl);
        } catch (Exception ex) {
          //catching exceptions here makes sure that the thread doesn't die in case of unexpected
          //exceptions
          LOG.warn("Exception received while cleaning notifications: ", ex);
        }

        LOG.debug("Cleaner thread done");
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOG.info("Cleaner thread sleep interrupted", e);
        }
      }
    }

    public void setTimeToLive(long configTtl) {
      if (configTtl > Integer.MAX_VALUE) {
        ttl = Integer.MAX_VALUE;
      } else {
        ttl = (int)configTtl;
      }
    }

    public void setCleanupInterval(long configInterval) {
      sleepTime = configInterval;
    }

  }
}
