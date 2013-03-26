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

package org.apache.hcatalog.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.thrift.TException;

import junit.framework.TestCase;

public class TestNotificationListener extends TestCase implements MessageListener{

	private HiveConf hiveConf;
	private Driver driver;
	private AtomicInteger cntInvocation = new AtomicInteger(0);

	@Override
	protected void setUp() throws Exception {

		super.setUp();
		System.setProperty("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
		System.setProperty("java.naming.provider.url", "vm://localhost?broker.persistent=false");
		ConnectionFactory connFac = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
		Connection conn = connFac.createConnection();
		conn.start();
		// We want message to be sent when session commits, thus we run in
		// transacted mode.
		Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
		Destination hcatTopic = session.createTopic(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX);
		MessageConsumer consumer1 = session.createConsumer(hcatTopic);
		consumer1.setMessageListener(this);
		Destination tblTopic = session.createTopic(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX+".mydb.mytbl");
		MessageConsumer consumer2 = session.createConsumer(tblTopic);
		consumer2.setMessageListener(this);
		Destination dbTopic = session.createTopic(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX+".mydb");
		MessageConsumer consumer3 = session.createConsumer(dbTopic);
		consumer3.setMessageListener(this);
		hiveConf = new HiveConf(this.getClass());
		hiveConf.set(ConfVars.METASTORE_EVENT_LISTENERS.varname,NotificationListener.class.getName());
		hiveConf.set("hive.metastore.local", "true");
		hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
		hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
		hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
		SessionState.start(new CliSessionState(hiveConf));
		driver = new Driver(hiveConf);
	}

	@Override
	protected void tearDown() throws Exception {
		assertEquals(7, cntInvocation.get());
		super.tearDown();
	}

	public void testAMQListener() throws MetaException, TException, UnknownTableException, NoSuchObjectException, 
	CommandNeedRetryException, UnknownDBException, InvalidPartitionException, UnknownPartitionException{
		driver.run("create database mydb");
		driver.run("use mydb");
		driver.run("create table mytbl (a string) partitioned by (b string)");
		driver.run("alter table mytbl add partition(b='2011')");
		HiveMetaStoreClient msc = new HiveMetaStoreClient(hiveConf);
		Map<String,String> kvs = new HashMap<String, String>(1);
		kvs.put("b", "2011");
		msc.markPartitionForEvent("mydb", "mytbl", kvs, PartitionEventType.LOAD_DONE);
		driver.run("alter table mytbl drop partition(b='2011')");
		driver.run("drop table mytbl");
		driver.run("drop database mydb");
	}

	@Override
	public void onMessage(Message msg) {
		cntInvocation.incrementAndGet();

		String event;
		try {
			event = msg.getStringProperty(HCatConstants.HCAT_EVENT);
			if(event.equals(HCatConstants.HCAT_ADD_DATABASE_EVENT)){

				assertEquals("topic://"+HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX,msg.getJMSDestination().toString());
				assertEquals("mydb", ((Database) ((ObjectMessage)msg).getObject()).getName());
			}
			else if(event.equals(HCatConstants.HCAT_ADD_TABLE_EVENT)){

				assertEquals("topic://hcat.mydb",msg.getJMSDestination().toString());
				Table tbl = (Table)(((ObjectMessage)msg).getObject());
				assertEquals("mytbl", tbl.getTableName());
				assertEquals("mydb", tbl.getDbName());
				assertEquals(1, tbl.getPartitionKeysSize());
			}
			else if(event.equals(HCatConstants.HCAT_ADD_PARTITION_EVENT)){

				assertEquals("topic://hcat.mydb.mytbl",msg.getJMSDestination().toString());
				Partition part = (Partition)(((ObjectMessage)msg).getObject());
				assertEquals("mytbl", part.getTableName());
				assertEquals("mydb", part.getDbName());
				List<String> vals = new ArrayList<String>(1);
				vals.add("2011");
				assertEquals(vals,part.getValues());
			}
			else if(event.equals(HCatConstants.HCAT_DROP_PARTITION_EVENT)){

				assertEquals("topic://hcat.mydb.mytbl",msg.getJMSDestination().toString());
				Partition part = (Partition)(((ObjectMessage)msg).getObject());
				assertEquals("mytbl", part.getTableName());
				assertEquals("mydb", part.getDbName());
				List<String> vals = new ArrayList<String>(1);
				vals.add("2011");
				assertEquals(vals,part.getValues());
			}
			else if(event.equals(HCatConstants.HCAT_DROP_TABLE_EVENT)){

				assertEquals("topic://hcat.mydb",msg.getJMSDestination().toString());
				Table tbl = (Table)(((ObjectMessage)msg).getObject());
				assertEquals("mytbl", tbl.getTableName());
				assertEquals("mydb", tbl.getDbName());
				assertEquals(1, tbl.getPartitionKeysSize());
			}
			else if(event.equals(HCatConstants.HCAT_DROP_DATABASE_EVENT)){

				assertEquals("topic://"+HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX,msg.getJMSDestination().toString());
				assertEquals("mydb", ((Database) ((ObjectMessage)msg).getObject()).getName());
			}
			else if (event.equals(HCatConstants.HCAT_PARTITION_DONE_EVENT)) {
				assertEquals("topic://hcat.mydb.mytbl",msg.getJMSDestination().toString());
				MapMessage mapMsg = (MapMessage)msg;
				assert mapMsg.getString("b").equals("2011");
			} else
				assert false;
		} catch (JMSException e) {
			e.printStackTrace(System.err);
			assert false;
		}
	}
}
