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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.listener;

import java.util.ArrayList;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.messaging.HCatEventMessage;
import org.apache.hcatalog.messaging.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of
 * {@link org.apache.hadoop.hive.metastore.MetaStoreEventListener} It sends
 * message on two type of topics. One has name of form dbName.tblName On this
 * topic, two kind of messages are sent: add/drop partition and
 * finalize_partition message. Second topic has name "HCAT" and messages sent on
 * it are: add/drop database and add/drop table. All messages also has a
 * property named "HCAT_EVENT" set on them whose value can be used to configure
 * message selector on subscriber side.
 * 
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.listener.NotificationListener} instead
 */
public class NotificationListener extends MetaStoreEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(NotificationListener.class);
  protected Session session;
  protected Connection conn;
  private static MessageFactory messageFactory = MessageFactory.getInstance();

  /**
   * Create message bus connection and session in constructor.
   */
  public NotificationListener(final Configuration conf) {

    super(conf);
    createConnection();
  }

  private static String getTopicName(Partition partition,
                     ListenerEvent partitionEvent) throws MetaException {
    try {
      return partitionEvent.getHandler()
        .get_table(partition.getDbName(), partition.getTableName())
        .getParameters().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME);
    } catch (NoSuchObjectException e) {
      throw new MetaException(e.toString());
    }
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent)
    throws MetaException {
    // Subscriber can get notification of newly add partition in a
    // particular table by listening on a topic named "dbName.tableName"
    // and message selector string as "HCAT_EVENT = HCAT_ADD_PARTITION"
    if (partitionEvent.getStatus()) {

      Partition partition = partitionEvent.getPartition();
      String topicName = getTopicName(partition, partitionEvent);
      if (topicName != null && !topicName.equals("")) {
        send(messageFactory.buildAddPartitionMessage(partitionEvent.getTable(), partition), topicName);
      } else {
        LOG.info("Topic name not found in metastore. Suppressing HCatalog notification for "
          + partition.getDbName()
          + "."
          + partition.getTableName()
          + " To enable notifications for this table, please do alter table set properties ("
          + HCatConstants.HCAT_MSGBUS_TOPIC_NAME
          + "=<dbname>.<tablename>) or whatever you want topic name to be.");
      }
    }

  }

  /**
   * Send dropped partition notifications. Subscribers can receive these notifications for a
   * particular table by listening on a topic named "dbName.tableName" with message selector
   * string {@value org.apache.hcatalog.common.HCatConstants#HCAT_EVENT} =
   * {@value org.apache.hcatalog.common.HCatConstants#HCAT_DROP_PARTITION_EVENT}.
   * </br>
   * TODO: DataNucleus 2.0.3, currently used by the HiveMetaStore for persistence, has been
   * found to throw NPE when serializing objects that contain null. For this reason we override
   * some fields in the StorageDescriptor of this notification. This should be fixed after
   * HIVE-2084 "Upgrade datanucleus from 2.0.3 to 3.0.1" is resolved.
   */
  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    if (partitionEvent.getStatus()) {
      Partition partition = partitionEvent.getPartition();
      StorageDescriptor sd = partition.getSd();
      sd.setBucketCols(new ArrayList<String>());
      sd.setSortCols(new ArrayList<Order>());
      sd.setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSkewedInfo().setSkewedColNames(new ArrayList<String>());
      String topicName = getTopicName(partition, partitionEvent);
      if (topicName != null && !topicName.equals("")) {
        send(messageFactory.buildDropPartitionMessage(partitionEvent.getTable(), partition), topicName);
      } else {
        LOG.info("Topic name not found in metastore. Suppressing HCatalog notification for "
          + partition.getDbName()
          + "."
          + partition.getTableName()
          + " To enable notifications for this table, please do alter table set properties ("
          + HCatConstants.HCAT_MSGBUS_TOPIC_NAME
          + "=<dbname>.<tablename>) or whatever you want topic name to be.");
      }
    }
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent)
    throws MetaException {
    // Subscriber can get notification about addition of a database in HCAT
    // by listening on a topic named "HCAT" and message selector string
    // as "HCAT_EVENT = HCAT_ADD_DATABASE"
    if (dbEvent.getStatus()) {
      String topicName = getTopicPrefix(dbEvent.getHandler().getHiveConf());
      send(messageFactory.buildCreateDatabaseMessage(dbEvent.getDatabase()), topicName);
    }
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    // Subscriber can get notification about drop of a database in HCAT
    // by listening on a topic named "HCAT" and message selector string
    // as "HCAT_EVENT = HCAT_DROP_DATABASE"
    if (dbEvent.getStatus())  {
      String topicName = getTopicPrefix(dbEvent.getHandler().getHiveConf());
      send(messageFactory.buildDropDatabaseMessage(dbEvent.getDatabase()), topicName);
    }
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    // Subscriber can get notification about addition of a table in HCAT
    // by listening on a topic named "HCAT" and message selector string
    // as "HCAT_EVENT = HCAT_ADD_TABLE"
    if (tableEvent.getStatus()) {
      Table tbl = tableEvent.getTable();
      HMSHandler handler = tableEvent.getHandler();
      HiveConf conf = handler.getHiveConf();
      Table newTbl;
      try {
        newTbl = handler.get_table(tbl.getDbName(), tbl.getTableName())
          .deepCopy();
        newTbl.getParameters().put(
          HCatConstants.HCAT_MSGBUS_TOPIC_NAME,
          getTopicPrefix(conf) + "." + newTbl.getDbName().toLowerCase() + "."
            + newTbl.getTableName().toLowerCase());
        handler.alter_table(newTbl.getDbName(), newTbl.getTableName(), newTbl);
      } catch (InvalidOperationException e) {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      } catch (NoSuchObjectException e) {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
      String topicName = getTopicPrefix(conf) + "." + newTbl.getDbName().toLowerCase();
      send(messageFactory.buildCreateTableMessage(newTbl), topicName);
    }
  }

  private String getTopicPrefix(HiveConf conf) {
    return conf.get(HCatConstants.HCAT_MSGBUS_TOPIC_PREFIX,
      HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX);
  }

  /**
   * Send dropped table notifications. Subscribers can receive these notifications for
   * dropped tables by listening on topic "HCAT" with message selector string
   * {@value org.apache.hcatalog.common.HCatConstants#HCAT_EVENT} =
   * {@value org.apache.hcatalog.common.HCatConstants#HCAT_DROP_TABLE_EVENT}
   * </br>
   * TODO: DataNucleus 2.0.3, currently used by the HiveMetaStore for persistence, has been
   * found to throw NPE when serializing objects that contain null. For this reason we override
   * some fields in the StorageDescriptor of this notification. This should be fixed after
   * HIVE-2084 "Upgrade datanucleus from 2.0.3 to 3.0.1" is resolved.
   */
  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    // Subscriber can get notification about drop of a table in HCAT
    // by listening on a topic named "HCAT" and message selector string
    // as "HCAT_EVENT = HCAT_DROP_TABLE"

    // Datanucleus throws NPE when we try to serialize a table object
    // retrieved from metastore. To workaround that we reset following objects

    if (tableEvent.getStatus()) {
      Table table = tableEvent.getTable();
      String topicName = getTopicPrefix(tableEvent.getHandler().getHiveConf()) + "." + table.getDbName().toLowerCase();
      send(messageFactory.buildDropTableMessage(table), topicName);
    }
  }

  /**
   * @param hCatEventMessage The HCatEventMessage being sent over JMS.
   * @param topicName is the name on message broker on which message is sent.
   */
  protected void send(HCatEventMessage hCatEventMessage, String topicName) {
    try {
      if(null == session){
        // this will happen, if we never able to establish a connection.
        createConnection();
        if (null == session){
          // Still not successful, return from here.
          LOG.error("Invalid session. Failed to send message on topic: " +
              topicName + " event: " + hCatEventMessage.getEventType());
          return;
        }
      }

      Destination topic = getTopic(topicName);

      if (null == topic){
        // Still not successful, return from here.
        LOG.error("Invalid session. Failed to send message on topic: " +
            topicName + " event: " + hCatEventMessage.getEventType());
        return;
      }

      MessageProducer producer = session.createProducer(topic);
      Message msg = session.createTextMessage(hCatEventMessage.toString());

      msg.setStringProperty(HCatConstants.HCAT_EVENT, hCatEventMessage.getEventType().toString());
      msg.setStringProperty(HCatConstants.HCAT_MESSAGE_VERSION, messageFactory.getVersion());
      msg.setStringProperty(HCatConstants.HCAT_MESSAGE_FORMAT, messageFactory.getMessageFormat());
      producer.send(msg);
      // Message must be transacted before we return.
      session.commit();
    }
    catch(Exception e){
      // Gobble up the exception. Message delivery is best effort.
      LOG.error("Failed to send message on topic: " + topicName +
          " event: " + hCatEventMessage.getEventType(), e);
    }
  }

  /**
   * Get the topic object for the topicName, it also tries to reconnect
   * if the connection appears to be broken.
   *
   * @param topicName The String identifying the message-topic.
   * @return A {@link Topic} object corresponding to the specified topicName.
   * @throws JMSException
   */
  protected Topic getTopic(final String topicName) throws JMSException {
    Topic topic;
    try {
      // Topics are created on demand. If it doesn't exist on broker it will
      // be created when broker receives this message.
      topic = session.createTopic(topicName);
    } catch (IllegalStateException ise) {
      // this will happen if we were able to establish connection once, but its no longer valid,
      // ise is thrown, catch it and retry.
      LOG.error("Seems like connection is lost. Retrying", ise);
      createConnection();
      topic = session.createTopic(topicName);
    }
    return topic;
  }

  protected void createConnection() {

    Context jndiCntxt;
    try {
      jndiCntxt = new InitialContext();
      ConnectionFactory connFac = (ConnectionFactory) jndiCntxt
        .lookup("ConnectionFactory");
      Connection conn = connFac.createConnection();
      conn.start();
      conn.setExceptionListener(new ExceptionListener() {
        @Override
        public void onException(JMSException jmse) {
          LOG.error(jmse.toString());
        }
      });
      // We want message to be sent when session commits, thus we run in
      // transacted mode.
      session = conn.createSession(true, Session.SESSION_TRANSACTED);
    } catch (NamingException e) {
      LOG.error("JNDI error while setting up Message Bus connection. "
        + "Please make sure file named 'jndi.properties' is in "
        + "classpath and contains appropriate key-value pairs.", e);
    } catch (JMSException e) {
      LOG.error("Failed to initialize connection to message bus", e);
    } catch (Throwable t) {
      LOG.error("Unable to connect to JMS provider", t);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    // Close the connection before dying.
    try {
      if (null != session)
        session.close();
      if (conn != null) {
        conn.close();
      }

    } catch (Exception ignore) {
      LOG.info("Failed to close message bus connection.", ignore);
    }
  }

  @Override
  public void onLoadPartitionDone(LoadPartitionDoneEvent lpde)
    throws MetaException {
//  TODO: Fix LoadPartitionDoneEvent. Currently, LPDE can only carry a single partition-spec. And that defeats the purpose.
//        if(lpde.getStatus())
//            send(lpde.getPartitionName(),lpde.getTable().getParameters().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME),HCatConstants.HCAT_PARTITION_DONE_EVENT);
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent ape) throws MetaException {
    // no-op
  }

  @Override
  public void onAlterTable(AlterTableEvent ate) throws MetaException {
    // no-op
  }
}
