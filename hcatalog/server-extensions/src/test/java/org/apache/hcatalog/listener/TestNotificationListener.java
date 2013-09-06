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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.mapreduce.HCatBaseTest;

import org.apache.hcatalog.messaging.AddPartitionMessage;
import org.apache.hcatalog.messaging.CreateDatabaseMessage;
import org.apache.hcatalog.messaging.CreateTableMessage;
import org.apache.hcatalog.messaging.DropDatabaseMessage;
import org.apache.hcatalog.messaging.DropPartitionMessage;
import org.apache.hcatalog.messaging.DropTableMessage;
import org.apache.hcatalog.messaging.HCatEventMessage;
import org.apache.hcatalog.messaging.MessageDeserializer;
import org.apache.hcatalog.messaging.MessageFactory;
import org.apache.hcatalog.messaging.jms.MessagingUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.listener.TestNotificationListener} instead
 */
public class TestNotificationListener extends HCatBaseTest implements MessageListener {

  private List<String> actualMessages = new ArrayList<String>();

  @Before
  public void setUp() throws Exception {
    System.setProperty("java.naming.factory.initial",
        "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
    System.setProperty("java.naming.provider.url",
        "vm://localhost?broker.persistent=false");
    ConnectionFactory connFac = new ActiveMQConnectionFactory(
        "vm://localhost?broker.persistent=false");
    Connection conn = connFac.createConnection();
    conn.start();
    // We want message to be sent when session commits, thus we run in
    // transacted mode.
    Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
    Destination hcatTopic = session
        .createTopic(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX);
    MessageConsumer consumer1 = session.createConsumer(hcatTopic);
    consumer1.setMessageListener(this);
    Destination tblTopic = session
        .createTopic(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX + ".mydb.mytbl");
    MessageConsumer consumer2 = session.createConsumer(tblTopic);
    consumer2.setMessageListener(this);
    Destination dbTopic = session
        .createTopic(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX + ".mydb");
    MessageConsumer consumer3 = session.createConsumer(dbTopic);
    consumer3.setMessageListener(this);

    setUpHiveConf();
    hiveConf.set(ConfVars.METASTORE_EVENT_LISTENERS.varname,
        NotificationListener.class.getName());
    SessionState.start(new CliSessionState(hiveConf));
    driver = new Driver(hiveConf);
    client = new HiveMetaStoreClient(hiveConf);
  }

  @After
  public void tearDown() throws Exception {
    List<String> expectedMessages = Arrays.asList(
        HCatConstants.HCAT_CREATE_DATABASE_EVENT,
        HCatConstants.HCAT_CREATE_TABLE_EVENT,
        HCatConstants.HCAT_ADD_PARTITION_EVENT,
        HCatConstants.HCAT_DROP_PARTITION_EVENT,
        HCatConstants.HCAT_DROP_TABLE_EVENT,
        HCatConstants.HCAT_DROP_DATABASE_EVENT);
    Assert.assertEquals(expectedMessages, actualMessages);
  }

  @Test
  public void testAMQListener() throws Exception {
    driver.run("create database mydb");
    driver.run("use mydb");
    driver.run("create table mytbl (a string) partitioned by (b string)");
    driver.run("alter table mytbl add partition(b='2011')");
    Map<String, String> kvs = new HashMap<String, String>(1);
    kvs.put("b", "2011");
    client.markPartitionForEvent("mydb", "mytbl", kvs,
        PartitionEventType.LOAD_DONE);
    driver.run("alter table mytbl drop partition(b='2011')");
    driver.run("drop table mytbl");
    driver.run("drop database mydb");
  }

  @Override
  public void onMessage(Message msg) {
    String event;
    try {
      event = msg.getStringProperty(HCatConstants.HCAT_EVENT);
      String format = msg.getStringProperty(HCatConstants.HCAT_MESSAGE_FORMAT);
      String version = msg.getStringProperty(HCatConstants.HCAT_MESSAGE_VERSION);
      String messageBody = ((TextMessage)msg).getText();
      actualMessages.add(event);
      MessageDeserializer deserializer = MessageFactory.getDeserializer(format, version);

      if (event.equals(HCatConstants.HCAT_CREATE_DATABASE_EVENT)) {

        Assert.assertEquals("topic://" + HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX, msg
            .getJMSDestination().toString());
        CreateDatabaseMessage message =  deserializer.getCreateDatabaseMessage(messageBody);
        Assert.assertEquals("mydb", message.getDB());
        HCatEventMessage message2 = MessagingUtils.getMessage(msg);
        Assert.assertTrue("Unexpected message-type.", message2 instanceof CreateDatabaseMessage);
        Assert.assertEquals("mydb", message2.getDB());
      } else if (event.equals(HCatConstants.HCAT_CREATE_TABLE_EVENT)) {

        Assert.assertEquals("topic://hcat.mydb", msg.getJMSDestination().toString());
        CreateTableMessage message = deserializer.getCreateTableMessage(messageBody);
        Assert.assertEquals("mytbl", message.getTable());
        Assert.assertEquals("mydb", message.getDB());
        HCatEventMessage message2 = MessagingUtils.getMessage(msg);
        Assert.assertTrue("Unexpected message-type.", message2 instanceof CreateTableMessage);
        Assert.assertEquals("mydb", message2.getDB());
        Assert.assertEquals("mytbl", ((CreateTableMessage) message2).getTable());
      } else if (event.equals(HCatConstants.HCAT_ADD_PARTITION_EVENT)) {

        Assert.assertEquals("topic://hcat.mydb.mytbl", msg.getJMSDestination()
            .toString());
        AddPartitionMessage message = deserializer.getAddPartitionMessage(messageBody);
        Assert.assertEquals("mytbl", message.getTable());
        Assert.assertEquals("mydb", message.getDB());
        Assert.assertEquals(1, message.getPartitions().size());
        Assert.assertEquals("2011", message.getPartitions().get(0).get("b"));
        HCatEventMessage message2 = MessagingUtils.getMessage(msg);
        Assert.assertTrue("Unexpected message-type.", message2 instanceof AddPartitionMessage);
        Assert.assertEquals("mydb", message2.getDB());
        Assert.assertEquals("mytbl", ((AddPartitionMessage) message2).getTable());
        Assert.assertEquals(1, ((AddPartitionMessage) message2).getPartitions().size());
        Assert.assertEquals("2011", ((AddPartitionMessage) message2).getPartitions().get(0).get("b"));
      } else if (event.equals(HCatConstants.HCAT_DROP_PARTITION_EVENT)) {

        Assert.assertEquals("topic://hcat.mydb.mytbl", msg.getJMSDestination()
            .toString());
        DropPartitionMessage message = deserializer.getDropPartitionMessage(messageBody);
        Assert.assertEquals("mytbl", message.getTable());
        Assert.assertEquals("mydb", message.getDB());
        Assert.assertEquals(1, message.getPartitions().size());
        Assert.assertEquals("2011", message.getPartitions().get(0).get("b"));
        HCatEventMessage message2 = MessagingUtils.getMessage(msg);
        Assert.assertTrue("Unexpected message-type.", message2 instanceof DropPartitionMessage);
        Assert.assertEquals("mydb", message2.getDB());
        Assert.assertEquals("mytbl", ((DropPartitionMessage) message2).getTable());
        Assert.assertEquals(1, ((DropPartitionMessage) message2).getPartitions().size());
        Assert.assertEquals("2011", ((DropPartitionMessage) message2).getPartitions().get(0).get("b"));
      } else if (event.equals(HCatConstants.HCAT_DROP_TABLE_EVENT)) {

        Assert.assertEquals("topic://hcat.mydb", msg.getJMSDestination().toString());
        DropTableMessage message = deserializer.getDropTableMessage(messageBody);
        Assert.assertEquals("mytbl", message.getTable());
        Assert.assertEquals("mydb", message.getDB());
        HCatEventMessage message2 = MessagingUtils.getMessage(msg);
        Assert.assertTrue("Unexpected message-type.", message2 instanceof DropTableMessage);
        Assert.assertEquals("mydb", message2.getDB());
        Assert.assertEquals("mytbl", ((DropTableMessage) message2).getTable());
      } else if (event.equals(HCatConstants.HCAT_DROP_DATABASE_EVENT)) {

        Assert.assertEquals("topic://" + HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX, msg
            .getJMSDestination().toString());
        DropDatabaseMessage message =  deserializer.getDropDatabaseMessage(messageBody);
        Assert.assertEquals("mydb", message.getDB());
        HCatEventMessage message2 = MessagingUtils.getMessage(msg);
        Assert.assertTrue("Unexpected message-type.", message2 instanceof DropDatabaseMessage);
        Assert.assertEquals("mydb", message2.getDB());
      } else if (event.equals(HCatConstants.HCAT_PARTITION_DONE_EVENT)) {
        // TODO: Fill in when PARTITION_DONE_EVENT is supported.
        Assert.assertTrue("Unexpected: HCAT_PARTITION_DONE_EVENT not supported (yet).", false);
      } else {
        Assert.assertTrue("Unexpected event-type: " + event, false);
      }

    } catch (JMSException e) {
      e.printStackTrace(System.err);
      assert false;
    }
  }
}
