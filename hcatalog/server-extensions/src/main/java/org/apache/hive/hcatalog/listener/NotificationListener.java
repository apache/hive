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

package org.apache.hive.hcatalog.listener;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.hive.hcatalog.messaging.MessageFactory;
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
 */
public class NotificationListener extends MetaStoreEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(NotificationListener.class);
  protected Connection conn;
  private static MessageFactory messageFactory = MessageFactory.getInstance();
  public static final int NUM_RETRIES = 1;
  private static final String HEALTH_CHECK_TOPIC_SUFFIX = "jms_health_check";
  private static final String HEALTH_CHECK_MSG = "HCAT_JMS_HEALTH_CHECK_MESSAGE";

  protected final ThreadLocal<Session> session = new ThreadLocal<Session>() {
    @Override
    protected Session initialValue() {
      try {
        return createSession();
      } catch (Exception e) {
        LOG.error("Couldn't create JMS Session", e);
        return null;
      }
    }

    @Override
    public void remove() {
      if (get() != null) {
        try {
          get().close();
        } catch (Exception e) {
          LOG.error("Unable to close bad JMS session, ignored error", e);
        }
      }
      super.remove();
    }
  };

  /**
   * Create message bus connection and session in constructor.
   */
  public NotificationListener(final Configuration conf) {
    super(conf);
    testAndCreateConnection();
  }

  private static String getTopicName(Table table) {
    return table.getParameters().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME);
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent)
    throws MetaException {
    // Subscriber can get notification of newly add partition in a
    // particular table by listening on a topic named "dbName.tableName"
    // and message selector string as "HCAT_EVENT = HCAT_ADD_PARTITION"
    if (partitionEvent.getStatus()) {
      Table table = partitionEvent.getTable();
      String topicName = getTopicName(table);
      if (topicName != null && !topicName.equals("")) {
        send(messageFactory.buildAddPartitionMessage(table, partitionEvent.getPartitionIterator()), topicName);
      } else {
        LOG.info("Topic name not found in metastore. Suppressing HCatalog notification for "
            + partitionEvent.getTable().getDbName()
            + "."
            + partitionEvent.getTable().getTableName()
            + " To enable notifications for this table, please do alter table set properties ("
            + HCatConstants.HCAT_MSGBUS_TOPIC_NAME
            + "=<dbname>.<tablename>) or whatever you want topic name to be.");
      }
    }
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent ape) throws MetaException {
    if (ape.getStatus()) {
      Partition before = ape.getOldPartition();
      Partition after = ape.getNewPartition();

      String topicName = getTopicName(ape.getTable());
      send(messageFactory.buildAlterPartitionMessage(ape.getTable(),before, after), topicName);
    }
  }

  /**
   * Send dropped partition notifications. Subscribers can receive these notifications for a
   * particular table by listening on a topic named "dbName.tableName" with message selector
   * string {@value org.apache.hive.hcatalog.common.HCatConstants#HCAT_EVENT} =
   * {@value org.apache.hive.hcatalog.common.HCatConstants#HCAT_DROP_PARTITION_EVENT}.
   * </br>
   * TODO: DataNucleus 2.0.3, currently used by the HiveMetaStore for persistence, has been
   * found to throw NPE when serializing objects that contain null. For this reason we override
   * some fields in the StorageDescriptor of this notification. This should be fixed after
   * HIVE-2084 "Upgrade datanucleus from 2.0.3 to 3.0.1" is resolved.
   */
  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    if (partitionEvent.getStatus()) {
      String topicName = getTopicName(partitionEvent.getTable());
      if (topicName != null && !topicName.equals("")) {
        send(messageFactory.buildDropPartitionMessage(partitionEvent.getTable(), partitionEvent.getPartitionIterator()), topicName);
      } else {
        LOG.info("Topic name not found in metastore. Suppressing HCatalog notification for "
          + partitionEvent.getTable().getDbName()
          + "."
          + partitionEvent.getTable().getTableName()
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
    if (dbEvent.getStatus()) {
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
        newTbl = handler.get_table_core(tbl.getDbName(), tbl.getTableName())
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

  private String getTopicPrefix(Configuration conf) {
    return conf.get(HCatConstants.HCAT_MSGBUS_TOPIC_PREFIX,
      HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX);
  }

  /**
   * Send altered table notifications. Subscribers can receive these notifications for
   * dropped tables by listening on topic "HCAT" with message selector string
   * {@value org.apache.hive.hcatalog.common.HCatConstants#HCAT_EVENT} =
   * {@value org.apache.hive.hcatalog.common.HCatConstants#HCAT_ALTER_TABLE_EVENT}
   */
  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
    if (tableEvent.getStatus()) {
      Table before = tableEvent.getOldTable();
      Table after = tableEvent.getNewTable();

      // onCreateTable alters the table to add the topic name.  Since this class is generating
      // that alter, we don't want to notify on that alter.  So take a quick look and see if
      // that's what this this alter is, and if so swallow it.
      if (after.getParameters() != null &&
          after.getParameters().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME) != null &&
          (before.getParameters() == null ||
              before.getParameters().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME) == null)) {
        return;
      }
      // I think this is wrong, the alter table statement should come on the table topic not the
      // DB topic - Alan.
      String topicName = getTopicPrefix(tableEvent.getHandler().getHiveConf()) + "." +
          after.getDbName().toLowerCase();
      send(messageFactory.buildAlterTableMessage(before, after), topicName);
    }
  }

  /**
   * Send dropped table notifications. Subscribers can receive these notifications for
   * dropped tables by listening on topic "HCAT" with message selector string
   * {@value org.apache.hive.hcatalog.common.HCatConstants#HCAT_EVENT} =
   * {@value org.apache.hive.hcatalog.common.HCatConstants#HCAT_DROP_TABLE_EVENT}
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
      // I think this is wrong, the drop table statement should come on the table topic not the
      // DB topic - Alan.
      String topicName = getTopicPrefix(tableEvent.getHandler().getHiveConf()) + "." + table.getDbName().toLowerCase();
      send(messageFactory.buildDropTableMessage(table), topicName);
    }
  }

  /**
   * @param hCatEventMessage The HCatEventMessage being sent over JMS.
   * @param topicName is the name on message broker on which message is sent.
   */
  protected void send(HCatEventMessage hCatEventMessage, String topicName) {
    send(hCatEventMessage, topicName, NUM_RETRIES);
  }

  /**
   * @param hCatEventMessage The HCatEventMessage being sent over JMS, this method is threadsafe
   * @param topicName is the name on message broker on which message is sent.
   * @param retries the number of retry attempts
   */
  protected void send(HCatEventMessage hCatEventMessage, String topicName, int retries) {
    try {
      if (session.get() == null) {
        // Need to reconnect
        throw new JMSException("Invalid JMS session");
      }
      Destination topic = createTopic(topicName);
      Message msg = session.get().createTextMessage(hCatEventMessage.toString());

      msg.setStringProperty(HCatConstants.HCAT_EVENT, hCatEventMessage.getEventType().toString());
      msg.setStringProperty(HCatConstants.HCAT_MESSAGE_VERSION, messageFactory.getVersion());
      msg.setStringProperty(HCatConstants.HCAT_MESSAGE_FORMAT, messageFactory.getMessageFormat());
      MessageProducer producer = createProducer(topic);
      producer.send(msg);
      // Message must be transacted before we return.
      session.get().commit();
    } catch (Exception e) {
      if (retries >= 0) {
        // this may happen if we were able to establish connection once, but its no longer valid
        LOG.error("Seems like connection is lost. Will retry. Retries left : " + retries + ". error was:", e);
        testAndCreateConnection();
        send(hCatEventMessage, topicName, retries - 1);
      } else {
        // Gobble up the exception. Message delivery is best effort.
        LOG.error("Failed to send message on topic: " + topicName +
          " event: " + hCatEventMessage.getEventType() + " after retries: " + NUM_RETRIES, e);
      }
    }
  }

  /**
   * Get the topic object for the topicName
   *
   * @param topicName The String identifying the message-topic.
   * @return A {@link Topic} object corresponding to the specified topicName.
   * @throws JMSException
   */
  protected Topic createTopic(final String topicName) throws JMSException {
    return session.get().createTopic(topicName);
  }

  /**
   * Does a health check on the connection by sending a dummy message.
   * Create the connection if the connection is found to be bad
   * Also recreates the session
   */
  protected synchronized void testAndCreateConnection() {
    if (conn != null) {
      // This method is reached when error occurs while sending msg, so the session must be bad
      session.remove();
      if (!isConnectionHealthy()) {
        // I am the first thread to detect the error, cleanup old connection & reconnect
        try {
          conn.close();
        } catch (Exception e) {
          LOG.error("Unable to close bad JMS connection, ignored error", e);
        }
        conn = createConnection();
      }
    } else {
      conn = createConnection();
    }
    try {
      session.set(createSession());
    } catch (JMSException e) {
      LOG.error("Couldn't create JMS session, ignored the error", e);
    }
  }

  /**
   * Create the JMS connection
   * @return newly created JMS connection
   */
  protected Connection createConnection() {
    LOG.info("Will create new JMS connection");
    Context jndiCntxt;
    Connection jmsConnection = null;
    try {
      jndiCntxt = new InitialContext();
      ConnectionFactory connFac = (ConnectionFactory) jndiCntxt.lookup("ConnectionFactory");
      jmsConnection = connFac.createConnection();
      jmsConnection.start();
      jmsConnection.setExceptionListener(new ExceptionListener() {
        @Override
        public void onException(JMSException jmse) {
          LOG.error("JMS Exception listener received exception. Ignored the error", jmse);
        }
      });
    } catch (NamingException e) {
      LOG.error("JNDI error while setting up Message Bus connection. "
        + "Please make sure file named 'jndi.properties' is in "
        + "classpath and contains appropriate key-value pairs.", e);
    } catch (JMSException e) {
      LOG.error("Failed to initialize connection to message bus", e);
    } catch (Throwable t) {
      LOG.error("Unable to connect to JMS provider", t);
    }
    return jmsConnection;
  }

  /**
   * Send a dummy message to probe if the JMS connection is healthy
   * @return true if connection is healthy, false otherwise
   */
  protected boolean isConnectionHealthy() {
    try {
      Topic topic = createTopic(getTopicPrefix(getConf()) + "." + HEALTH_CHECK_TOPIC_SUFFIX);
      MessageProducer producer = createProducer(topic);
      Message msg = session.get().createTextMessage(HEALTH_CHECK_MSG);
      producer.send(msg, DeliveryMode.NON_PERSISTENT, 4, 0);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  /**
   * Creates a JMS session
   * @return newly create JMS session
   * @throws JMSException
   */
  protected Session createSession() throws JMSException {
    // We want message to be sent when session commits, thus we run in
    // transacted mode.
    return conn.createSession(true, Session.SESSION_TRANSACTED);
  }

  /**
   * Create a JMS producer
   * @param topic
   * @return newly created message producer
   * @throws JMSException
   */
  protected MessageProducer createProducer(Destination topic) throws JMSException {
    return session.get().createProducer(topic);
  }

  @Override
  protected void finalize() throws Throwable {
    if (conn != null) {
      try {
        conn.close();
      } catch (Exception e) {
        LOG.error("Couldn't close jms connection, ignored the error", e);
      }
    }
  }

  @Override
  public void onLoadPartitionDone(LoadPartitionDoneEvent lpde)
    throws MetaException {
//  TODO: Fix LoadPartitionDoneEvent. Currently, LPDE can only carry a single partition-spec. And that defeats the purpose.
//        if(lpde.getStatus())
//            send(lpde.getPartitionName(),lpde.getTable().getParameters().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME),HCatConstants.HCAT_PARTITION_DONE_EVENT);
  }
}
