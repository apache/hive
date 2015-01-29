/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RawStoreProxy;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.messaging.MessageFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

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
public class DbNotificationListener extends MetaStoreEventListener {

  private static final Log LOG = LogFactory.getLog(DbNotificationListener.class.getName());
  private static CleanerThread cleaner = null;

  // This is the same object as super.conf, but it's convenient to keep a copy of it as a
  // HiveConf rather than a Configuration.
  private HiveConf hiveConf;
  private MessageFactory msgFactory;
  private RawStore rs;

  private synchronized void init(HiveConf conf) {
    try {
      rs = RawStoreProxy.getProxy(conf, conf,
          conf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL), 999999);
    } catch (MetaException e) {
      LOG.error("Unable to connect to raw store, notifications will not be tracked", e);
      rs = null;
    }
    if (cleaner == null && rs != null) {
      cleaner = new CleanerThread(conf, rs);
      cleaner.start();
    }
  }

  public DbNotificationListener(Configuration config) {
    super(config);
    // The code in MetastoreUtils.getMetaStoreListeners() that calls this looks for a constructor
    // with a Configuration parameter, so we have to declare config as Configuration.  But it
    // actually passes a HiveConf, which we need.  So we'll do this ugly down cast.
    hiveConf = (HiveConf)config;
    init(hiveConf);
    msgFactory = MessageFactory.getInstance();
  }

  /**
   * @param tableEvent table event.
   * @throws org.apache.hadoop.hive.metastore.api.MetaException
   */
  public void onConfigChange(ConfigChangeEvent tableEvent) throws MetaException {
    String key = tableEvent.getKey();
    if (key.equals(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL.toString())) {
      // This weirdness of setting it in our hiveConf and then reading back does two things.
      // One, it handles the conversion of the TimeUnit.  Two, it keeps the value around for
      // later in case we need it again.
      hiveConf.set(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL.name(),
          tableEvent.getNewValue());
      cleaner.setTimeToLive(hiveConf.getTimeVar(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL,
          TimeUnit.SECONDS));
    }
  }

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  public void onCreateTable (CreateTableEvent tableEvent) throws MetaException {
    Table t = tableEvent.getTable();
    NotificationEvent event = new NotificationEvent(0, now(),
        HCatConstants.HCAT_CREATE_TABLE_EVENT, msgFactory.buildCreateTableMessage(t).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    enqueue(event);
  }

  /**
   * @param tableEvent table event.
   * @throws MetaException
   */
  public void onDropTable (DropTableEvent tableEvent)  throws MetaException {
    Table t = tableEvent.getTable();
    NotificationEvent event = new NotificationEvent(0, now(),
        HCatConstants.HCAT_DROP_TABLE_EVENT, msgFactory.buildDropTableMessage(t).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    enqueue(event);
  }

  /**
   * @param tableEvent alter table event
   * @throws MetaException
   */
  public void onAlterTable (AlterTableEvent tableEvent) throws MetaException {
    Table before = tableEvent.getOldTable();
    Table after = tableEvent.getNewTable();
    NotificationEvent event = new NotificationEvent(0, now(),
        HCatConstants.HCAT_ALTER_TABLE_EVENT,
        msgFactory.buildAlterTableMessage(before, after).toString());
    if (event != null) {
      event.setDbName(after.getDbName());
      event.setTableName(after.getTableName());
      enqueue(event);
    }
  }

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  public void onAddPartition (AddPartitionEvent partitionEvent)
      throws MetaException {
    Table t = partitionEvent.getTable();
    NotificationEvent event = new NotificationEvent(0, now(),
        HCatConstants.HCAT_ADD_PARTITION_EVENT,
        msgFactory.buildAddPartitionMessage(t, partitionEvent.getPartitions()).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    enqueue(event);
  }

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  public void onDropPartition (DropPartitionEvent partitionEvent)  throws MetaException {
    Table t = partitionEvent.getTable();
    NotificationEvent event = new NotificationEvent(0, now(),
        HCatConstants.HCAT_DROP_PARTITION_EVENT,
        msgFactory.buildDropPartitionMessage(t, partitionEvent.getPartition()).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    enqueue(event);
  }

  /**
   * @param partitionEvent partition event
   * @throws MetaException
   */
  public void onAlterPartition (AlterPartitionEvent partitionEvent)  throws MetaException {
    Partition before = partitionEvent.getOldPartition();
    Partition after = partitionEvent.getNewPartition();
    NotificationEvent event = new NotificationEvent(0, now(),
        HCatConstants.HCAT_ALTER_PARTITION_EVENT,
        msgFactory.buildAlterPartitionMessage(before, after).toString());
    if (event != null) {
      event.setDbName(before.getDbName());
      event.setTableName(before.getTableName());
      enqueue(event);
    }
  }

  /**
   * @param dbEvent database event
   * @throws MetaException
   */
  public void onCreateDatabase (CreateDatabaseEvent dbEvent) throws MetaException {
    Database db = dbEvent.getDatabase();
    NotificationEvent event = new NotificationEvent(0, now(),
        HCatConstants.HCAT_CREATE_DATABASE_EVENT,
        msgFactory.buildCreateDatabaseMessage(db).toString());
    event.setDbName(db.getName());
    enqueue(event);
  }

  /**
   * @param dbEvent database event
   * @throws MetaException
   */
  public void onDropDatabase (DropDatabaseEvent dbEvent) throws MetaException {
    Database db = dbEvent.getDatabase();
    NotificationEvent event = new NotificationEvent(0, now(),
        HCatConstants.HCAT_DROP_DATABASE_EVENT,
        msgFactory.buildDropDatabaseMessage(db).toString());
    event.setDbName(db.getName());
    enqueue(event);
  }

  /**
   * @param partSetDoneEvent
   * @throws MetaException
   */
  public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {
    // TODO, we don't support this, but we should, since users may create an empty partition and
    // then load data into it.

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

  private void enqueue(NotificationEvent event) {
    if (rs != null) {
      rs.addNotificationEvent(event);
    } else {
      LOG.warn("Dropping event " + event + " since notification is not running.");
    }
  }

  private static class CleanerThread extends Thread {
    private RawStore rs;
    private int ttl;


    CleanerThread(HiveConf conf, RawStore rs) {
      super("CleanerThread");
      this.rs = rs;
      setTimeToLive(conf.getTimeVar(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL,
          TimeUnit.SECONDS));
      setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        rs.cleanNotificationEvents(ttl);
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          LOG.info("Cleaner thread sleep interupted", e);
        }
      }
    }

    public void setTimeToLive(long configTtl) {
      if (configTtl > Integer.MAX_VALUE) ttl = Integer.MAX_VALUE;
      else ttl = (int)configTtl;
    }

  }

}
