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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HMSHandler;
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
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.AddCheckConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddDefaultConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddForeignKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddNotNullConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AddPrimaryKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddUniqueConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionsEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.BatchAcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.CommitCompactionEvent;
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
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEventBatch;
import org.apache.hadoop.hive.metastore.events.UpdateTableColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.DeleteTableColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.DeletePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.ReloadEvent;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.AcidWriteMessage;
import org.apache.hadoop.hive.metastore.messaging.AddCheckConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddDefaultConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddForeignKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddNotNullConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPrimaryKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddUniqueConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AllocWriteIdMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionsMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitCompactionMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.DropConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.DropDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.DropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.MessageSerializer;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.PartitionFiles;
import org.apache.hadoop.hive.metastore.messaging.UpdateTableColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.DeleteTableColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.UpdatePartitionColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.DeletePartitionColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.ReloadMessage;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.hcatalog.data.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

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

  private static final String NL_SEL_SQL = "select \"NEXT_VAL\" from \"SEQUENCE_TABLE\" where \"SEQUENCE_NAME\" = ?";
  private static final String NL_UPD_SQL = "update \"SEQUENCE_TABLE\" set \"NEXT_VAL\" = ? where \"SEQUENCE_NAME\" = ?";

  private static final String EV_SEL_SQL = "select \"NEXT_EVENT_ID\" from \"NOTIFICATION_SEQUENCE\"";
  private static final String EV_UPD_SQL =  "update \"NOTIFICATION_SEQUENCE\" set \"NEXT_EVENT_ID\" = ?";

  private static CleanerThread cleaner = null;

  private int maxBatchSize;

  private Configuration conf;
  private MessageEncoder msgEncoder;

  //cleaner is a static object, use static synchronized to make sure its thread-safe
  private static synchronized void init(Configuration conf) throws MetaException {
    if (cleaner == null) {
      cleaner =
          new CleanerThread(conf, RawStoreProxy.getProxy(conf, conf,
              MetastoreConf.getVar(conf, ConfVars.RAW_STORE_IMPL)));
      cleaner.start();
    }
  }

  @VisibleForTesting
  public static synchronized void resetCleaner(HiveConf conf) throws Exception {
    if (cleaner != null && conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL)) {
      cleaner.interrupt();
      cleaner.join();
      cleaner = null;
      init(conf);
    }
  }

  public DbNotificationListener(Configuration config) throws MetaException {
    super(config);
    conf = config;
    DbNotificationListener.init(conf);
    msgEncoder = MessageFactory.getDefaultInstance(conf);
    maxBatchSize = MetastoreConf.getIntVar(conf, ConfVars.JDBC_MAX_BATCH_SIZE);
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
      boolean isReplEnabled = MetastoreConf.getBoolVar(getConf(), ConfVars.REPLCMENABLED);
      if(!isReplEnabled){
        cleaner.setTimeToLive(MetastoreConf.getTimeVar(getConf(), ConfVars.EVENT_DB_LISTENER_TTL,
                TimeUnit.SECONDS));
      }
    } else if (key.equals(ConfVars.REPL_EVENT_DB_LISTENER_TTL.toString()) ||
            key.equals(ConfVars.REPL_EVENT_DB_LISTENER_TTL.getHiveName())) {
      long time = MetastoreConf.convertTimeStr(tableEvent.getNewValue(), TimeUnit.SECONDS,
              TimeUnit.SECONDS);
      boolean isReplEnabled = MetastoreConf.getBoolVar(getConf(), ConfVars.REPLCMENABLED);
      if(isReplEnabled){
        cleaner.setTimeToLive(time);
      }
    }

    if (key.equals(ConfVars.REPLCMENABLED.toString()) || key.equals(ConfVars.REPLCMENABLED.getHiveName())) {
      boolean isReplEnabled = MetastoreConf.getBoolVar(conf, ConfVars.REPLCMENABLED);
      if(isReplEnabled){
        cleaner.setTimeToLive(MetastoreConf.getTimeVar(conf, ConfVars.REPL_EVENT_DB_LISTENER_TTL,
                TimeUnit.SECONDS));
      }
      else {
        cleaner.setTimeToLive(MetastoreConf.getTimeVar(conf, ConfVars.EVENT_DB_LISTENER_TTL,
                TimeUnit.SECONDS));
      }
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

    if (key.equals(EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL.toString()) || key
        .equals(EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL.getHiveName())) {
      cleaner.setWaitInterval(MetastoreConf
          .getTimeVar(getConf(), EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL,
              TimeUnit.MILLISECONDS));
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
    CreateTableMessage msg =
        MessageBuilder.getInstance().buildCreateTableMessage(t, fileIter);
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.CREATE_TABLE.toString(),
            msgEncoder.getSerializer().serialize(msg));
    event.setCatName(t.isSetCatName() ? t.getCatName() : getDefaultCatalog(conf));
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
    DropTableMessage msg = MessageBuilder.getInstance().buildDropTableMessage(t);
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_TABLE.toString(),
            msgEncoder.getSerializer().serialize(msg));
    event.setCatName(t.isSetCatName() ? t.getCatName() : getDefaultCatalog(conf));
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
    AlterTableMessage msg = MessageBuilder.getInstance()
        .buildAlterTableMessage(before, after, tableEvent.getIsTruncateOp(),
            tableEvent.getWriteId());
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ALTER_TABLE.toString(),
            msgEncoder.getSerializer().serialize(msg)
        );
    event.setCatName(after.isSetCatName() ? after.getCatName() : getDefaultCatalog(conf));
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
        return ReplChangeManager.getInstance(conf).encodeFileUri(file.getPath().toString(),
            ReplChangeManager.checksumFor(file.getPath(), fs), null);
      } catch (IOException | MetaException e) {
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
    EventMessage msg = MessageBuilder.getInstance()
        .buildAddPartitionMessage(t, partitionEvent.getPartitionIterator(), fileIter);
    MessageSerializer serializer = msgEncoder.getSerializer();

    NotificationEvent event = new NotificationEvent(0, now(),
        EventType.ADD_PARTITION.toString(), serializer.serialize(msg));
    event.setCatName(t.isSetCatName() ? t.getCatName() : getDefaultCatalog(conf));
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
    DropPartitionMessage msg =
        MessageBuilder.getInstance()
            .buildDropPartitionMessage(t, partitionEvent.getPartitionIterator());
    NotificationEvent event = new NotificationEvent(0, now(), EventType.DROP_PARTITION.toString(),
        msgEncoder.getSerializer().serialize(msg));
    event.setCatName(t.isSetCatName() ? t.getCatName() : getDefaultCatalog(conf));
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
    AlterPartitionMessage msg = MessageBuilder.getInstance()
        .buildAlterPartitionMessage(partitionEvent.getTable(), before, after,
            partitionEvent.getIsTruncateOp(),
            partitionEvent.getWriteId());
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ALTER_PARTITION.toString(),
            msgEncoder.getSerializer().serialize(msg));
    event.setCatName(before.isSetCatName() ? before.getCatName() : getDefaultCatalog(conf));
    event.setDbName(before.getDbName());
    event.setTableName(before.getTableName());
    process(event, partitionEvent);
  }

  @Override
  public void onAlterPartitions(AlterPartitionsEvent event) throws MetaException {
    Table table = event.getTable();
    Iterator<List<Partition>> iterator = event.getNewPartsIterator(maxBatchSize);
    while (iterator.hasNext()) {
      List<Partition> partitions = iterator.next();
      AlterPartitionsMessage msg = MessageBuilder.getInstance()
          .buildAlterPartitionsMessage(event.getTable(), partitions,
              event.getIsTruncateOp(), partitions.get(0).getWriteId());
      NotificationEvent notification =
          new NotificationEvent(0, now(), EventType.ALTER_PARTITIONS.toString(),
              msgEncoder.getSerializer().serialize(msg));
      notification.setCatName(table.isSetCatName() ? table.getCatName() : getDefaultCatalog(conf));
      notification.setDbName(table.getDbName());
      notification.setTableName(table.getTableName());
      process(notification, event);
    }
  }

  /**
   * @param dbEvent database event
   * @throws MetaException
   */
  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
    Database db = dbEvent.getDatabase();
    CreateDatabaseMessage msg = MessageBuilder.getInstance()
        .buildCreateDatabaseMessage(db);
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.CREATE_DATABASE.toString(),
            msgEncoder.getSerializer().serialize(msg));
    event.setCatName(db.isSetCatalogName() ? db.getCatalogName() : getDefaultCatalog(conf));
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
    DropDatabaseMessage msg = MessageBuilder.getInstance()
        .buildDropDatabaseMessage(db);
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_DATABASE.toString(),
            msgEncoder.getSerializer().serialize(msg));
    event.setCatName(db.isSetCatalogName() ? db.getCatalogName() : getDefaultCatalog(conf));
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
    AlterDatabaseMessage msg = MessageBuilder.getInstance()
        .buildAlterDatabaseMessage(oldDb, newDb);
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ALTER_DATABASE.toString(),
            msgEncoder.getSerializer().serialize(msg)
        );
    event.setCatName(oldDb.isSetCatalogName() ? oldDb.getCatalogName() : getDefaultCatalog(conf));
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
    CreateFunctionMessage msg = MessageBuilder.getInstance()
        .buildCreateFunctionMessage(fn);
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.CREATE_FUNCTION.toString(),
            msgEncoder.getSerializer().serialize(msg));
    event.setCatName(fn.isSetCatName() ? fn.getCatName() : getDefaultCatalog(conf));
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
    DropFunctionMessage msg = MessageBuilder.getInstance().buildDropFunctionMessage(fn);
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_FUNCTION.toString(),
            msgEncoder.getSerializer().serialize(msg));
    event.setCatName(fn.isSetCatName() ? fn.getCatName() : getDefaultCatalog(conf));
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
        result = ReplChangeManager.getInstance(conf).
                encodeFileUri(files.get(i), (chksums != null && !chksums.isEmpty()) ? chksums.get(i) : null,
                subDirs != null ? subDirs.get(i) : null);
      } catch (IOException | MetaException e) {
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
    InsertMessage msg = MessageBuilder.getInstance().buildInsertMessage(tableObj,
        insertEvent.getPartitionObj(), insertEvent.isReplace(),
        new FileChksumIterator(insertEvent.getFiles(), insertEvent.getFileChecksums()));
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.INSERT.toString(),
            msgEncoder.getSerializer().serialize(msg));
    event.setCatName(tableObj.isSetCatName() ? tableObj.getCatName() : getDefaultCatalog(conf));
    event.setDbName(tableObj.getDbName());
    event.setTableName(tableObj.getTableName());
    process(event, insertEvent);
  }

  @Override
  public void onOpenTxn(OpenTxnEvent openTxnEvent, Connection dbConn, SQLGenerator sqlGenerator) throws MetaException {
    if (openTxnEvent.getTxnType() == TxnType.READ_ONLY) {
      return;
    }
    int lastTxnIdx = openTxnEvent.getTxnIds().size() - 1;
    OpenTxnMessage msg =
        MessageBuilder.getInstance().buildOpenTxnMessage(openTxnEvent.getTxnIds().get(0),
            openTxnEvent.getTxnIds().get(lastTxnIdx));
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.OPEN_TXN.toString(),
            msgEncoder.getSerializer().serialize(msg));

    try {
      addNotificationLog(event, openTxnEvent, dbConn, sqlGenerator);
    } catch (SQLException e) {
      throw new MetaException("Unable to execute direct SQL " + StringUtils.stringifyException(e));
    }
  }

  @Override
  public void onCommitTxn(CommitTxnEvent commitTxnEvent, Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
    if (commitTxnEvent.getTxnType() == TxnType.READ_ONLY) {
      return;
    }
    CommitTxnMessage msg =
        MessageBuilder.getInstance().buildCommitTxnMessage(commitTxnEvent.getTxnId());

    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.COMMIT_TXN.toString(),
            msgEncoder.getSerializer().serialize(msg));

    try {
      addNotificationLog(event, commitTxnEvent, dbConn, sqlGenerator);
    } catch (SQLException e) {
      throw new MetaException("Unable to execute direct SQL " + StringUtils.stringifyException(e));
    }
  }

  @Override
  public void onAbortTxn(AbortTxnEvent abortTxnEvent, Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
    if (abortTxnEvent.getTxnType() == TxnType.READ_ONLY) {
      return;
    }
    AbortTxnMessage msg =
        MessageBuilder.getInstance().buildAbortTxnMessage(abortTxnEvent.getTxnId(), abortTxnEvent.getDbsUpdated());
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ABORT_TXN.toString(),
            msgEncoder.getSerializer().serialize(msg));

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
      AddPrimaryKeyMessage msg = MessageBuilder.getInstance()
          .buildAddPrimaryKeyMessage(addPrimaryKeyEvent.getPrimaryKeyCols());
      NotificationEvent event = new NotificationEvent(0, now(), EventType.ADD_PRIMARYKEY.toString(),
          msgEncoder.getSerializer().serialize(msg));
      event.setCatName(cols.get(0).isSetCatName() ? cols.get(0).getCatName() : getDefaultCatalog(conf));
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
      AddForeignKeyMessage msg = MessageBuilder.getInstance()
          .buildAddForeignKeyMessage(addForeignKeyEvent.getForeignKeyCols());
      NotificationEvent event =
          new NotificationEvent(0, now(), EventType.ADD_FOREIGNKEY.toString(),
              msgEncoder.getSerializer().serialize(msg));
      event.setCatName(cols.get(0).isSetCatName() ? cols.get(0).getCatName() : getDefaultCatalog(conf));
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
      AddUniqueConstraintMessage msg = MessageBuilder.getInstance()
          .buildAddUniqueConstraintMessage(addUniqueConstraintEvent.getUniqueConstraintCols());
      NotificationEvent event =
          new NotificationEvent(0, now(), EventType.ADD_UNIQUECONSTRAINT.toString(),
              msgEncoder.getSerializer().serialize(msg)
          );
      event.setCatName(cols.get(0).isSetCatName() ? cols.get(0).getCatName() : getDefaultCatalog(conf));
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
      AddNotNullConstraintMessage msg = MessageBuilder.getInstance()
          .buildAddNotNullConstraintMessage(addNotNullConstraintEvent.getNotNullConstraintCols());
      NotificationEvent event =
          new NotificationEvent(0, now(), EventType.ADD_NOTNULLCONSTRAINT.toString(),
              msgEncoder.getSerializer().serialize(msg)
          );
      event.setCatName(cols.get(0).isSetCatName() ? cols.get(0).getCatName() : getDefaultCatalog(conf));
      event.setDbName(cols.get(0).getTable_db());
      event.setTableName(cols.get(0).getTable_name());
      process(event, addNotNullConstraintEvent);
    }
  }

  /***
   * @param addDefaultConstraintEvent add default constraint event
   * @throws MetaException
   */
  @Override
  public void onAddDefaultConstraint(AddDefaultConstraintEvent addDefaultConstraintEvent) throws MetaException {
    List<SQLDefaultConstraint> cols = addDefaultConstraintEvent.getDefaultConstraintCols();
    if (cols.size() > 0) {
      AddDefaultConstraintMessage colsInMsg = MessageBuilder.getInstance()
        .buildAddDefaultConstraintMessage(cols);
      NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ADD_DEFAULTCONSTRAINT.toString(),
          msgEncoder.getSerializer().serialize(colsInMsg)
        );
      event.setCatName(cols.get(0).isSetCatName() ? cols.get(0).getCatName() : getDefaultCatalog(conf));
      event.setDbName(cols.get(0).getTable_db());
      event.setTableName(cols.get(0).getTable_name());
      process(event, addDefaultConstraintEvent);
    }
  }

  /***
   * @param addCheckConstraintEvent add check constraint event
   * @throws MetaException
   */
  @Override
  public void onAddCheckConstraint(AddCheckConstraintEvent addCheckConstraintEvent) throws MetaException {
    LOG.info("Inside DBNotification listener for check constraint.");
    List<SQLCheckConstraint> cols = addCheckConstraintEvent.getCheckConstraintCols();
    if (cols.size() > 0) {
      AddCheckConstraintMessage colsInMsg = MessageBuilder.getInstance()
        .buildAddCheckConstraintMessage(cols);
      NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ADD_CHECKCONSTRAINT.toString(),
          msgEncoder.getSerializer().serialize(colsInMsg)
        );
      event.setCatName(cols.get(0).isSetCatName() ? cols.get(0).getCatName() : getDefaultCatalog(conf));
      event.setDbName(cols.get(0).getTable_db());
      event.setTableName(cols.get(0).getTable_name());
      process(event, addCheckConstraintEvent);
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
    DropConstraintMessage msg = MessageBuilder.getInstance()
        .buildDropConstraintMessage(dbName, tableName, constraintName);
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.DROP_CONSTRAINT.toString(),
            msgEncoder.getSerializer().serialize(msg));
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
    AllocWriteIdMessage msg = MessageBuilder.getInstance()
        .buildAllocWriteIdMessage(allocWriteIdEvent.getTxnToWriteIdList(), dbName, tableName);
    NotificationEvent event =
        new NotificationEvent(0, now(), EventType.ALLOC_WRITE_ID.toString(),
            msgEncoder.getSerializer().serialize(msg)
        );
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
    AcidWriteMessage msg = MessageBuilder.getInstance().buildAcidWriteMessage(acidWriteEvent,
            new FileChksumIterator(acidWriteEvent.getFiles(), acidWriteEvent.getChecksums(),
                    acidWriteEvent.getSubDirs()));
    NotificationEvent event = new NotificationEvent(0, now(), EventType.ACID_WRITE.toString(),
            msgEncoder.getSerializer().serialize(msg));
    event.setMessageFormat(msgEncoder.getMessageFormat());
    event.setDbName(acidWriteEvent.getDatabase());
    event.setTableName(acidWriteEvent.getTable());
    try {
      addWriteNotificationLog(Collections.singletonList(event), Collections.singletonList(acidWriteEvent),
              dbConn, sqlGenerator, Collections.singletonList(msg));
    } catch (SQLException e) {
      throw new MetaException("Unable to add write notification log " + StringUtils.stringifyException(e));
    }
  }

  @Override
  public void onBatchAcidWrite(BatchAcidWriteEvent batchAcidWriteEvent, Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
    List<NotificationEvent> eventBatch = new ArrayList<>();
    List<AcidWriteMessage> msgBatch = new ArrayList<>();
    List<AcidWriteEvent> acidWriteEventList = new ArrayList<>();
    for (int i = 0; i < batchAcidWriteEvent.getNumRequest(); i++) {
      AcidWriteEvent acidWriteEvent = new AcidWriteEvent(batchAcidWriteEvent.getPartition(i),
              batchAcidWriteEvent.getTableObj(i), batchAcidWriteEvent.getPartitionObj(i),
              batchAcidWriteEvent.getNotificationRequest(i));
      AcidWriteMessage msg = MessageBuilder.getInstance().buildAcidWriteMessage(acidWriteEvent,
              new FileChksumIterator(batchAcidWriteEvent.getFiles(i), batchAcidWriteEvent.getChecksums(i),
                      batchAcidWriteEvent.getSubDirs(i)));
      NotificationEvent event = new NotificationEvent(0, now(), EventType.ACID_WRITE.toString(),
              msgEncoder.getSerializer().serialize(msg));
      event.setMessageFormat(msgEncoder.getMessageFormat());
      event.setDbName(batchAcidWriteEvent.getDatabase(i));
      event.setTableName(batchAcidWriteEvent.getTable(i));
      eventBatch.add(event);
      msgBatch.add(msg);
      acidWriteEventList.add(acidWriteEvent);
    }
    try {
      addWriteNotificationLog(eventBatch, acidWriteEventList, dbConn, sqlGenerator, msgBatch);
    } catch (SQLException e) {
      throw new MetaException("Unable to add batch write notification log " + StringUtils.stringifyException(e));
    }
  }

  @Override
  public void onUpdateTableColumnStat(UpdateTableColumnStatEvent updateTableColumnStatEvent) throws MetaException {
    UpdateTableColumnStatMessage msg = MessageBuilder.getInstance()
            .buildUpdateTableColumnStatMessage(updateTableColumnStatEvent.getColStats(),
                    updateTableColumnStatEvent.getTableObj(),
                    updateTableColumnStatEvent.getTableParameters(),
                    updateTableColumnStatEvent.getWriteId());
    NotificationEvent event = new NotificationEvent(0, now(), EventType.UPDATE_TABLE_COLUMN_STAT.toString(),
                    msgEncoder.getSerializer().serialize(msg));
    ColumnStatisticsDesc statDesc = updateTableColumnStatEvent.getColStats().getStatsDesc();
    event.setCatName(statDesc.isSetCatName() ? statDesc.getCatName() : getDefaultCatalog(conf));
    event.setDbName(statDesc.getDbName());
    event.setTableName(statDesc.getTableName());
    process(event, updateTableColumnStatEvent);
  }

  @Override
  public void onDeleteTableColumnStat(DeleteTableColumnStatEvent deleteTableColumnStatEvent) throws MetaException {
    DeleteTableColumnStatMessage msg = MessageBuilder.getInstance()
            .buildDeleteTableColumnStatMessage(deleteTableColumnStatEvent.getDBName(),
                    deleteTableColumnStatEvent.getColName());
    NotificationEvent event = new NotificationEvent(0, now(), EventType.DELETE_TABLE_COLUMN_STAT.toString(),
                    msgEncoder.getSerializer().serialize(msg));
    event.setCatName(deleteTableColumnStatEvent.getCatName());
    event.setDbName(deleteTableColumnStatEvent.getDBName());
    event.setTableName(deleteTableColumnStatEvent.getTableName());
    process(event, deleteTableColumnStatEvent);
  }

  @Override
  public void onUpdatePartitionColumnStat(UpdatePartitionColumnStatEvent updatePartColStatEvent) throws MetaException {
    UpdatePartitionColumnStatMessage msg = MessageBuilder.getInstance()
            .buildUpdatePartitionColumnStatMessage(updatePartColStatEvent.getPartColStats(),
                    updatePartColStatEvent.getPartVals(),
                    updatePartColStatEvent.getPartParameters(),
                    updatePartColStatEvent.getTableObj(),
                    updatePartColStatEvent.getWriteId());
    NotificationEvent event = new NotificationEvent(0, now(), EventType.UPDATE_PARTITION_COLUMN_STAT.toString(),
                    msgEncoder.getSerializer().serialize(msg));
    ColumnStatisticsDesc statDesc = updatePartColStatEvent.getPartColStats().getStatsDesc();
    event.setCatName(statDesc.isSetCatName() ? statDesc.getCatName() : getDefaultCatalog(conf));
    event.setDbName(statDesc.getDbName());
    event.setTableName(statDesc.getTableName());
    process(event, updatePartColStatEvent);
  }

  @Override
  public void onUpdatePartitionColumnStatInBatch(UpdatePartitionColumnStatEventBatch updatePartColStatEventBatch,
                                                 Connection dbConn, SQLGenerator sqlGenerator)
          throws MetaException {
    List<NotificationEvent> eventBatch = new ArrayList<>();
    List<ListenerEvent> listenerEventBatch = new ArrayList<>();
    for (int i = 0; i < updatePartColStatEventBatch.getNumEntries(); i++) {
      UpdatePartitionColumnStatEvent updatePartColStatEvent = updatePartColStatEventBatch.getPartColStatEvent(i);
      UpdatePartitionColumnStatMessage msg = MessageBuilder.getInstance()
              .buildUpdatePartitionColumnStatMessage(updatePartColStatEvent.getPartColStats(),
                      updatePartColStatEvent.getPartVals(),
                      updatePartColStatEvent.getPartParameters(),
                      updatePartColStatEvent.getTableObj(),
                      updatePartColStatEvent.getWriteId());

      ColumnStatisticsDesc statDesc = updatePartColStatEvent.getPartColStats().getStatsDesc();
      NotificationEvent event =
              new NotificationEvent(0, now(), EventType.UPDATE_PARTITION_COLUMN_STAT.toString(),
                      msgEncoder.getSerializer().serialize(msg));
      event.setCatName(statDesc.isSetCatName() ? statDesc.getCatName() : getDefaultCatalog(conf));
      event.setDbName(statDesc.getDbName());
      event.setTableName(statDesc.getTableName());
      eventBatch.add(event);
      listenerEventBatch.add(updatePartColStatEvent);
    }
    try {
      addNotificationLogBatch(eventBatch, listenerEventBatch, dbConn, sqlGenerator);
    } catch (SQLException e) {
      throw new MetaException("Unable to execute direct SQL " + StringUtils.stringifyException(e));
    }
  }

  @Override
  public void onDeletePartitionColumnStat(DeletePartitionColumnStatEvent deletePartColStatEvent) throws MetaException {
    DeletePartitionColumnStatMessage msg = MessageBuilder.getInstance()
            .buildDeletePartitionColumnStatMessage(deletePartColStatEvent.getDBName(),
                    deletePartColStatEvent.getColName(), deletePartColStatEvent.getPartName(),
                    deletePartColStatEvent.getPartVals());
    NotificationEvent event = new NotificationEvent(0, now(), EventType.DELETE_PARTITION_COLUMN_STAT.toString(),
                    msgEncoder.getSerializer().serialize(msg));
    event.setCatName(deletePartColStatEvent.getCatName());
    event.setDbName(deletePartColStatEvent.getDBName());
    event.setTableName(deletePartColStatEvent.getTableName());
    process(event, deletePartColStatEvent);
  }

  @Override
  public void onReload(ReloadEvent reloadEvent) throws MetaException {
    Table tableObj = reloadEvent.getTableObj();
    ReloadMessage msg = MessageBuilder.getInstance().buildReloadMessage(tableObj,
            reloadEvent.getPartitionObj(), reloadEvent.isRefreshEvent());
    NotificationEvent event =
            new NotificationEvent(0, now(), EventType.RELOAD.toString(),
                    msgEncoder.getSerializer().serialize(msg));
    event.setCatName(tableObj.isSetCatName() ? tableObj.getCatName() : getDefaultCatalog(conf));
    event.setDbName(tableObj.getDbName());
    event.setTableName(tableObj.getTableName());
    process(event, reloadEvent);
  }

  @Override
  public void onCommitCompaction(CommitCompactionEvent commitCompactionEvent, Connection dbConn, SQLGenerator sqlGenerator)
      throws MetaException {

    CommitCompactionMessage msg = MessageBuilder.getInstance().buildCommitCompactionMessage(commitCompactionEvent);

    NotificationEvent event = new NotificationEvent(0, now(), EventType.COMMIT_COMPACTION.toString(),
        msgEncoder.getSerializer().serialize(msg));
    event.setDbName(commitCompactionEvent.getDbname());
    event.setTableName(commitCompactionEvent.getTableName());

    try {
      addNotificationLog(event, commitCompactionEvent, dbConn, sqlGenerator);
    } catch (SQLException e) {
      throw new MetaException("Unable to execute direct SQL " + StringUtils.stringifyException(e));
    }
  }

  @Override
  public boolean doesAddEventsToNotificationLogTable() {
    return true;
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

  /**
   * Get the next notification log ID.
   *
   * @return The next ID to use for a notification log message
   * @throws SQLException if a database access error occurs or this method is
   *           called on a closed connection
   * @throws MetaException if the sequence table is not properly initialized
   */
  private long getNextNLId(Connection con, SQLGenerator sqlGenerator, String sequence, long numValues)
          throws SQLException, MetaException {

    final String sfuSql = sqlGenerator.addForUpdateClause(NL_SEL_SQL);
    Optional<Long> nextSequenceValue = Optional.empty();

    LOG.debug("Going to execute query [{}][1={}]", sfuSql, sequence);
    try (PreparedStatement stmt = con.prepareStatement(sfuSql)) {
      stmt.setString(1, sequence);
      ResultSet rs = stmt.executeQuery();
      if (rs.next()) {
        nextSequenceValue = Optional.of(rs.getLong(1));
      }
    }

    final long updatedNLId = numValues + nextSequenceValue.orElseThrow(
        () -> new MetaException("Transaction database not properly configured, failed to determine next NL ID"));

    LOG.debug("Going to execute query [{}][1={}][2={}]", NL_UPD_SQL, updatedNLId, sequence);
    try (PreparedStatement stmt = con.prepareStatement(NL_UPD_SQL)) {
      stmt.setLong(1, updatedNLId);
      stmt.setString(2, sequence);
      final int rowCount = stmt.executeUpdate();
      LOG.debug("Updated {} rows for sequnce {}", rowCount, sequence);
    }

    return nextSequenceValue.get();
  }

  /**
   * Get the next event ID.
   *
   * @return The next ID to use for an event.
   * @throws SQLException if a database access error occurs or this method is
   *           called on a closed connection
   * @throws MetaException if the sequence table is not properly initialized
   */
  private long getNextEventId(Connection con, SQLGenerator sqlGenerator, long numValues)
      throws SQLException, MetaException {

    /*
     * FOR UPDATE means something different in Derby than in most other vendor
     * implementations therefore it is required to lock the entire table. Since
     * there is only one row in the table, this should not cause any performance
     * degradation.
     *
     * see: https://db.apache.org/derby/docs/10.1/ref/rrefsqlj31783.html
     */
    if (sqlGenerator.getDbProduct().isDERBY()) {
      final String lockingQuery = sqlGenerator.lockTable("NOTIFICATION_SEQUENCE", false);
      LOG.debug("Locking Derby table [{}]", lockingQuery);
      try (Statement stmt = con.createStatement()) {
        stmt.execute(lockingQuery);
      }
    }

    final String sfuSql = sqlGenerator.addForUpdateClause(EV_SEL_SQL);
    Optional<Long> nextSequenceValue = Optional.empty();

    LOG.debug("Going to execute query [{}]", sfuSql);
    try (Statement stmt = con.createStatement()) {
      ResultSet rs = stmt.executeQuery(sfuSql);
      if (rs.next()) {
        nextSequenceValue = Optional.of(rs.getLong(1));
      }
    }

    final long updatedEventId = numValues + nextSequenceValue.orElseThrow(
        () -> new MetaException("Transaction database not properly configured, failed to determine next event ID"));

    LOG.debug("Going to execute query [{}][1={}]", EV_UPD_SQL, updatedEventId);
    try (PreparedStatement stmt = con.prepareStatement(EV_UPD_SQL)) {
      stmt.setLong(1, updatedEventId);
      final int rowCount = stmt.executeUpdate();
      LOG.debug("Updated {} rows for NOTIFICATION_SEQUENCE table", rowCount);
    }

    return nextSequenceValue.get();
  }

  private void addWriteNotificationLog(List<NotificationEvent> eventBatch, List<AcidWriteEvent> acidWriteEventList,
                                       Connection dbConn, SQLGenerator sqlGenerator, List<AcidWriteMessage> msgBatch)
          throws MetaException, SQLException {
    LOG.debug("DbNotificationListener: adding write notification log for : {}", eventBatch);
    assert ((dbConn != null) && (sqlGenerator != null));

    int numRows;
    long maxRows = MetastoreConf.getIntVar(conf, ConfVars.JDBC_MAX_BATCH_SIZE);

    try (Statement stmt = dbConn.createStatement()) {
      String st = sqlGenerator.getDbProduct().getPrepareTxnStmt();
      if (st != null) {
        stmt.execute(st);
      }
    } catch (Exception e) {
      LOG.error("Failed to execute query ", e);
      throw new MetaException(e.getMessage());
    }

    ResultSet rs = null;
    String select = sqlGenerator.addForUpdateClause("select \"WNL_ID\", \"WNL_FILES\" from" +
            " \"TXN_WRITE_NOTIFICATION_LOG\" " +
            "where \"WNL_DATABASE\" = ? " +
            "and \"WNL_TABLE\" = ? " + " and \"WNL_PARTITION\" = ? " +
            "and \"WNL_TXNID\" = ? ");
    List<Integer> insertList = new ArrayList<>();
    Map<Integer, Pair<Long, String>> updateMap = new HashMap<>();
    try (PreparedStatement pst = dbConn.prepareStatement(select)) {
      for (int i = 0; i < acidWriteEventList.size(); i++) {
        String dbName = acidWriteEventList.get(i).getDatabase();
        String tblName = acidWriteEventList.get(i).getTable();
        String partition = acidWriteEventList.get(i).getPartition();
        Long txnId = acidWriteEventList.get(i).getTxnId();

        LOG.debug("Going to execute query <" + select.replaceAll("\\?", "{}") + ">",
                quoteString(dbName), quoteString(tblName), quoteString(partition));
        pst.setString(1, dbName);
        pst.setString(2, tblName);
        pst.setString(3, partition);
        pst.setLong(4, txnId);
        rs = pst.executeQuery();
        if (!rs.next()) {
          insertList.add(i);
        } else {
          updateMap.put(i, new Pair<>(rs.getLong(1), rs.getString(2)));
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to execute insert ", e);
      throw new MetaException(e.getMessage());
    } finally {
      close(rs);
    }

    if (insertList.size() != 0) {
      // if rs is empty then no lock is taken and thus it can not cause deadlock.
      long nextNLId = getNextNLId(dbConn, sqlGenerator,
              "org.apache.hadoop.hive.metastore.model.MTxnWriteNotificationLog", insertList.size());

      String insert = "insert into \"TXN_WRITE_NOTIFICATION_LOG\" " +
              "(\"WNL_ID\", \"WNL_TXNID\", \"WNL_WRITEID\", \"WNL_DATABASE\", \"WNL_TABLE\", " +
              "\"WNL_PARTITION\", \"WNL_TABLE_OBJ\", \"WNL_PARTITION_OBJ\", " +
              "\"WNL_FILES\", \"WNL_EVENT_TIME\") VALUES (?,?,?,?,?,?,?,?,?,?)";
      try (PreparedStatement pst = dbConn.prepareStatement(sqlGenerator.addEscapeCharacters(insert))) {
        numRows = 0;
        for (int idx : insertList) {
          String tableObj = msgBatch.get(idx).getTableObjStr();
          String partitionObj = msgBatch.get(idx).getPartitionObjStr();
          String files = ReplChangeManager.joinWithSeparator(msgBatch.get(idx).getFiles());
          String dbName = acidWriteEventList.get(idx).getDatabase();
          String tblName = acidWriteEventList.get(idx).getTable();
          String partition = acidWriteEventList.get(idx).getPartition();
          int currentTime = now();

          pst.setLong(1, nextNLId++);
          pst.setLong(2, acidWriteEventList.get(idx).getTxnId());
          pst.setLong(3, acidWriteEventList.get(idx).getWriteId());
          pst.setString(4, dbName);
          pst.setString(5, tblName);
          pst.setString(6, partition);
          pst.setString(7, tableObj);
          pst.setString(8, partitionObj);
          pst.setString(9, files);
          pst.setInt(10, currentTime);
          LOG.debug("Going to execute insert <" + insert.replaceAll("\\?", "{}") + ">", nextNLId
                  , acidWriteEventList.get(idx).getTxnId(), acidWriteEventList.get(idx).getWriteId()
                  , quoteString(dbName), quoteString(tblName),
                  quoteString(partition), quoteString(tableObj), quoteString(partitionObj), quoteString(files), currentTime);
          pst.addBatch();
          numRows++;
          if (numRows == maxRows) {
            pst.executeBatch();
            numRows = 0;
          }
        }

        if (numRows != 0) {
          pst.executeBatch();
        }
      } catch (Exception e) {
        LOG.error("Failed to execute insert ", e);
        throw new MetaException(e.getMessage());
      }
    }

    if (updateMap.size() != 0) {
      String update = "update \"TXN_WRITE_NOTIFICATION_LOG\" set \"WNL_TABLE_OBJ\" = ? ," +
                " \"WNL_PARTITION_OBJ\" = ? ," +
                " \"WNL_FILES\" = ? ," +
                " \"WNL_EVENT_TIME\" = ?" +
                " where \"WNL_ID\" = ?";
      try (PreparedStatement pst = dbConn.prepareStatement(sqlGenerator.addEscapeCharacters(update))) {
        numRows = 0;
        for (Map.Entry entry : updateMap.entrySet()) {
          int idx = (int) entry.getKey();
          Pair<Long, String> nlIdInfo = (Pair<Long, String>) entry.getValue();
          String tableObj = msgBatch.get(idx).getTableObjStr();
          String partitionObj = msgBatch.get(idx).getPartitionObjStr();
          String files = ReplChangeManager.joinWithSeparator(msgBatch.get(idx).getFiles());
          String existingFiles = nlIdInfo.second;
          long nlId = nlIdInfo.first;
          int currentTime = now();

          if (existingFiles.contains(sqlGenerator.addEscapeCharacters(files))) {
            // If list of files are already present then no need to update it again. This scenario can come in case of
            // retry done to the meta store for the same operation.
            LOG.info("file list " + files + " already present");
            continue;
          }

          files = ReplChangeManager.joinWithSeparator(Lists.newArrayList(files, existingFiles));

          pst.setString(1, tableObj);
          pst.setString(2, partitionObj);
          pst.setString(3, files);
          pst.setInt(4, currentTime);
          pst.setLong(5, nlId);
          LOG.debug("Going to execute update <" + update.replaceAll("\\?", "{}") + ">",
                  quoteString(tableObj), quoteString(partitionObj), quoteString(files), currentTime, nlId);
          pst.addBatch();
          numRows++;
          if (numRows == maxRows) {
            pst.executeBatch();
            numRows = 0;
          }
        }

        if (numRows != 0) {
          pst.executeBatch();
        }
      } catch (Exception e) {
        LOG.error("Failed to execute update ", e);
        throw new MetaException(e.getMessage());
      }
    }
  }

  static String quoteString(String input) {
    return "'" + input + "'";
  }

  private void addNotificationLog(NotificationEvent event, ListenerEvent listenerEvent, Connection dbConn,
                                  SQLGenerator sqlGenerator) throws MetaException, SQLException {
    LOG.debug("DbNotificationListener: adding notification log for : {}", event.getMessage());
    addNotificationLogBatch(Collections.singletonList(event), Collections.singletonList(listenerEvent),
            dbConn, sqlGenerator);
  }

  private void addNotificationLogBatch(List<NotificationEvent> eventList, List<ListenerEvent> listenerEventList,
                                 Connection dbConn, SQLGenerator sqlGenerator) throws MetaException, SQLException {
    if ((dbConn == null) || (sqlGenerator == null)) {
      LOG.info("connection or sql generator is not set so executing sql via DN");
      for (int idx = 0; idx < eventList.size(); idx++) {
        LOG.debug("DbNotificationListener: adding notification log for : {}", eventList.get(idx).getMessage());
        process(eventList.get(idx), listenerEventList.get(idx));
      }
      return;
    }

    try (Statement stmt = dbConn.createStatement()) {
      if (sqlGenerator.getDbProduct().isMYSQL()) {
        stmt.execute("SET @@session.sql_mode=ANSI_QUOTES");
      }
    }

    long nextEventId = getNextEventId(dbConn, sqlGenerator, eventList.size());
    long nextNLId = getNextNLId(dbConn, sqlGenerator,
            "org.apache.hadoop.hive.metastore.model.MNotificationLog", eventList.size());

    String columns = "\"NL_ID\"" + ", \"EVENT_ID\"" + ", \"EVENT_TIME\"" + ", \"EVENT_TYPE\"" + ", \"MESSAGE\""
            + ", \"MESSAGE_FORMAT\"" + ", \"DB_NAME\"" + ", \"TBL_NAME\"" + ", \"CAT_NAME\"";
    String insertVal = "insert into \"NOTIFICATION_LOG\" (" + columns + ") VALUES ("
            + "?,?,?,?,?,?,?,?,?"
            + ")";

    try (PreparedStatement pst = dbConn.prepareStatement(insertVal)) {
      int numRows = 0;

      for (int idx = 0; idx < eventList.size(); idx++) {
        NotificationEvent event = eventList.get(idx);
        ListenerEvent listenerEvent = listenerEventList.get(idx);

        LOG.debug("DbNotificationListener: adding notification log for : {}", event.getMessage());
        event.setMessageFormat(msgEncoder.getMessageFormat());

        pst.setLong(1, nextNLId);
        pst.setLong(2, nextEventId);
        pst.setLong(3, event.getEventTime());
        pst.setString(4, event.getEventType());
        pst.setString(5, event.getMessage());
        pst.setString(6, event.getMessageFormat());
        pst.setString(7, event.getDbName());
        pst.setString(8, event.getTableName());
        pst.setString(9, event.getCatName());

        LOG.debug("Going to execute insert <" + insertVal + ">");
        pst.addBatch();
        numRows++;

        if (numRows == maxBatchSize) {
          pst.executeBatch();
          numRows = 0;
        }

        event.setEventId(nextEventId);
        // Set the DB_NOTIFICATION_EVENT_ID for future reference by other listeners.
        if (event.isSetEventId()) {
          listenerEvent.putParameter(
                  MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME,
                  Long.toString(event.getEventId()));
        }
        nextNLId++;
        nextEventId++;
      }
      if (numRows != 0) {
        pst.executeBatch();
      }
    } catch (SQLException e) {
      LOG.warn("failed to add notification log ", e);
      throw e;
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
    event.setMessageFormat(msgEncoder.getMessageFormat());
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
    private final RawStore rs;
    private int ttl;
    private long sleepTime;
    private long waitInterval;
    private boolean isInTest;

    CleanerThread(Configuration conf, RawStore rs) {
      super("DB-Notification-Cleaner");
      setDaemon(true);
      this.rs = Objects.requireNonNull(rs);

      boolean isReplEnabled = MetastoreConf.getBoolVar(conf, ConfVars.REPLCMENABLED);
      isInTest = conf.getBoolean(HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname, false);
      ConfVars ttlConf = (isReplEnabled) ?  ConfVars.REPL_EVENT_DB_LISTENER_TTL : ConfVars.EVENT_DB_LISTENER_TTL;
      setTimeToLive(MetastoreConf.getTimeVar(conf, ttlConf, TimeUnit.SECONDS));
      setCleanupInterval(
          MetastoreConf.getTimeVar(conf, ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL, TimeUnit.MILLISECONDS));
      setWaitInterval(MetastoreConf
          .getTimeVar(conf, EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL, TimeUnit.MILLISECONDS));
    }

    @Override
    public void run() {
      LOG.info("Wait interval is {}", waitInterval);
      if (waitInterval > 0) {
        try {
          LOG.info("Cleaner Thread Restarted and {} or {} is configured. So cleaner thread will startup post waiting "
                  + "{} ms", EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL,
              EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL.getHiveName(), waitInterval);
          Thread.sleep(waitInterval);
        } catch (InterruptedException e) {
          LOG.error("Failed during the initial wait before start.", e);
          if(isInTest) {
            Thread.currentThread().interrupt();
          }
          return;
        }
        LOG.info("Completed Cleaner thread initial wait. Starting normal processing.");
      }

      while (true) {
        LOG.debug("Cleaner thread running");
        try {
          rs.cleanNotificationEvents(ttl);
          rs.cleanWriteNotificationEvents(ttl);
        } catch (Exception ex) {
          LOG.warn("Exception received while cleaning notifications", ex);
        }
        LOG.debug("Cleaner thread done");

        try {
          LOG.debug("Sleeping {}ms", sleepTime);
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOG.info("Cleaner thread interrupted. Exiting.");
          return;
        }
      }
    }

    public void setTimeToLive(long configTtl) {
      this.ttl = (int) Math.min(Integer.MAX_VALUE, configTtl);
    }

    public void setCleanupInterval(long configInterval) {
      sleepTime = configInterval;
    }

    public void setWaitInterval(long waitInterval) {
      this.waitInterval = waitInterval;
    }
  }
}
