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
import org.apache.hadoop.hive.metastore.api.Index;
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
import org.apache.hadoop.hive.metastore.events.AddIndexEvent;
import org.apache.hadoop.hive.metastore.events.AddNotNullConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AddPrimaryKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddUniqueConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropConstraintEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropIndexEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.PartitionFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

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

  private synchronized void init(Configuration conf) throws MetaException {
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
    init(conf);
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
  }

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    Table t = tableEvent.getTable();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.CREATE_TABLE.toString(), msgFactory
            .buildCreateTableMessage(t, new FileIterator(t.getSd().getLocation())).toString());
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
            .buildAlterTableMessage(before, after, tableEvent.getIsTruncateOp()).toString());
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
            ReplChangeManager.checksumFor(file.getPath(), fs));
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
        List<String> files = Lists.newArrayList(new FileIterator(p.getSd().getLocation()));
        PartitionFiles partitionFiles =
            new PartitionFiles(Warehouse.makePartName(t.getPartitionKeys(), p.getValues()),
            files.iterator());
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
    String msg = msgFactory
        .buildAddPartitionMessage(t, partitionEvent.getPartitionIterator(),
            new PartitionFilesIterator(partitionEvent.getPartitionIterator(), t)).toString();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ADD_PARTITION.toString(), msg);
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
            .buildAlterPartitionMessage(partitionEvent.getTable(), before, after, partitionEvent.getIsTruncateOp()).toString());
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
    event.setDbName(fn.getDbName());
    process(event, fnEvent);
  }

  /**
   * @param indexEvent index event
   * @throws MetaException
   */
  @Override
  public void onAddIndex(AddIndexEvent indexEvent) throws MetaException {
    Index index = indexEvent.getIndex();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.CREATE_INDEX.toString(), msgFactory
            .buildCreateIndexMessage(index).toString());
    event.setDbName(index.getDbName());
    process(event, indexEvent);
  }

  /**
   * @param indexEvent index event
   * @throws MetaException
   */
  @Override
  public void onDropIndex(DropIndexEvent indexEvent) throws MetaException {
    Index index = indexEvent.getIndex();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_INDEX.toString(), msgFactory
            .buildDropIndexMessage(index).toString());
    event.setDbName(index.getDbName());
    process(event, indexEvent);
  }

  /**
   * @param indexEvent index event
   * @throws MetaException
   */
  @Override
  public void onAlterIndex(AlterIndexEvent indexEvent) throws MetaException {
    Index before = indexEvent.getOldIndex();
    Index after = indexEvent.getNewIndex();
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ALTER_INDEX.toString(), msgFactory
            .buildAlterIndexMessage(before, after).toString());
    event.setDbName(before.getDbName());
    process(event, indexEvent);
  }

  class FileChksumIterator implements Iterator<String> {
    private List<String> files;
    private List<String> chksums;
    int i = 0;
    FileChksumIterator(List<String> files, List<String> chksums) {
      this.files = files;
      this.chksums = chksums;
    }
    @Override
    public boolean hasNext() {
      return i< files.size();
    }

    @Override
    public String next() {
      String result = encodeFileUri(files.get(i), chksums != null? chksums.get(i) : null);
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
    event.setDbName(tableObj.getDbName());
    event.setTableName(tableObj.getTableName());
    process(event, insertEvent);
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
    event.setDbName(dbName);
    event.setTableName(tableName);
    process(event, dropConstraintEvent);
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
    static private long sleepTime = 60000;

    CleanerThread(Configuration conf, RawStore rs) {
      super("CleanerThread");
      this.rs = rs;
      setTimeToLive(MetastoreConf.getTimeVar(conf, ConfVars.EVENT_DB_LISTENER_TTL,
          TimeUnit.SECONDS));
      setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        rs.cleanNotificationEvents(ttl);
        LOG.debug("Cleaner thread done");
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOG.info("Cleaner thread sleep interrupted", e);
        }
      }
    }

    public void setTimeToLive(long configTtl) {
      if (configTtl > Integer.MAX_VALUE) ttl = Integer.MAX_VALUE;
      else ttl = (int)configTtl;
    }

  }

  // TODO: this needs to be enhanced once change management based filesystem is implemented
  // Currently using fileuri#checksum as the format
  private String encodeFileUri(String fileUriStr, String fileChecksum) {
    if (fileChecksum != null) {
      return fileUriStr + "#" + fileChecksum;
    } else {
      return fileUriStr;
    }
  }
}
