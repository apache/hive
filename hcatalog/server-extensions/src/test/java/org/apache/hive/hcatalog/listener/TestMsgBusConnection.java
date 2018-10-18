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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.hive.hcatalog.messaging.jms.MessagingUtils;
import org.junit.Before;
import org.junit.Test;

public class TestMsgBusConnection {

  private IDriver driver;
  private BrokerService broker;
  private MessageConsumer consumer;
  private static final int TIMEOUT = 2000;

  @Before
  public void before() throws Exception {

    broker = new BrokerService();
    // configure the broker
    broker.addConnector("tcp://localhost:61616?broker.persistent=false");

    broker.start();

    System.setProperty("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
    System.setProperty("java.naming.provider.url", "tcp://localhost:61616");
    connectClient();
    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.set(ConfVars.METASTORE_EVENT_LISTENERS.varname, NotificationListener.class.getName());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.set(HCatConstants.HCAT_MSGBUS_TOPIC_PREFIX, "planetlab.hcat");
    SessionState.start(new CliSessionState(hiveConf));
    driver = DriverFactory.newDriver(hiveConf);
  }

  private void connectClient() throws JMSException {
    ConnectionFactory connFac = new ActiveMQConnectionFactory("tcp://localhost:61616");
    Connection conn = connFac.createConnection();
    conn.start();
    Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
    Destination hcatTopic = session.createTopic("planetlab.hcat");
    consumer = session.createConsumer(hcatTopic);
  }

  @Test
  public void testConnection() throws Exception {
    driver.run("create database testconndb");
    Message msg = consumer.receive(TIMEOUT);
    assertTrue("Expected TextMessage", msg instanceof TextMessage);
    assertEquals(HCatConstants.HCAT_CREATE_DATABASE_EVENT, msg.getStringProperty(HCatConstants.HCAT_EVENT));
    assertEquals("topic://planetlab.hcat", msg.getJMSDestination().toString());
    HCatEventMessage messageObject = MessagingUtils.getMessage(msg);
    assertEquals("testconndb", messageObject.getDB());
    broker.stop();
    runQuery("drop database testconndb cascade");
    broker.start(true);
    connectClient();
    runQuery("create database testconndb");
    msg = consumer.receive(TIMEOUT);
    assertEquals(HCatConstants.HCAT_CREATE_DATABASE_EVENT, msg.getStringProperty(HCatConstants.HCAT_EVENT));
    assertEquals("topic://planetlab.hcat", msg.getJMSDestination().toString());
    assertEquals("testconndb", messageObject.getDB());
    driver.run("drop database testconndb cascade");
    msg = consumer.receive(TIMEOUT);
    assertEquals(HCatConstants.HCAT_DROP_DATABASE_EVENT, msg.getStringProperty(HCatConstants.HCAT_EVENT));
    assertEquals("topic://planetlab.hcat", msg.getJMSDestination().toString());
    assertEquals("testconndb", messageObject.getDB());
  }

  private void runQuery(String query) throws Exception {
    CommandProcessorResponse cpr = driver.run(query);
    assertFalse(cpr.getMessage(), cpr.failed());
  }
}
