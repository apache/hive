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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.MetaStoreEventListenerConstants;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.BatchAcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.DropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.api.repl.ReplicationV1CompatRule;
import org.apache.hive.hcatalog.data.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Ignore;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;


public class TestTransactionalDbNotificationListner {
    private static final Logger LOG = LoggerFactory.getLogger(TestTransactionalDbNotificationListner.class
            .getName());
    private static IMetaStoreClient msClient;
    private static IDriver driver;
    private static MessageDeserializer md;
    private int startTime;
    private long firstEventId;

    static {
        try {
            md = MessageFactory.getInstance(JSONMessageEncoder.FORMAT).getDeserializer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    @BeforeClass
    public static void connectToMetastore() throws Exception {
        HiveConf conf = new HiveConf();
        conf.setVar(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS,
                "org.apache.hive.hcatalog.listener.DbNotificationListener");
        conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        conf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
        conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL, DummyRawStoreFailEvent.class.getName());
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY, JSONMessageEncoder.class.getName());
        conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
                "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
        SessionState.start(new CliSessionState(conf));
        TestTxnDbUtil.setConfValues(conf);
        TestTxnDbUtil.prepDb(conf);
        msClient = new HiveMetaStoreClient(conf);
        driver = DriverFactory.newDriver(conf);
        md = JSONMessageEncoder.getInstance().getDeserializer();
    }

    @Before
    public void setup() throws Exception {
        long now = System.currentTimeMillis() / 1000;
        startTime = 0;
        if (now > Integer.MAX_VALUE) {
            fail("Bummer, time has fallen over the edge");
        } else {
            startTime = (int) now;
        }
        firstEventId = msClient.getCurrentNotificationEventId().getEventId();
        DummyRawStoreFailEvent.setEventSucceed(true);
    }

    @Test
    public void openTxn() throws Exception {
        msClient.openTxn("me", TxnType.READ_ONLY);
        NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
        assertEquals(0, rsp.getEventsSize());

        msClient.openTxn("me", TxnType.DEFAULT);
        rsp = msClient.getNextNotification(firstEventId, 0, null);
        assertEquals(1, rsp.getEventsSize());

        NotificationEvent event = rsp.getEvents().get(0);
        assertEquals(firstEventId + 1, event.getEventId());
        assertTrue(event.getEventTime() >= startTime);
        assertEquals(EventType.OPEN_TXN.toString(), event.getEventType());
    }

    @Test
    public void abortTxn() throws Exception {

        long txnId1 = msClient.openTxn("me", TxnType.READ_ONLY);
        long txnId2 = msClient.openTxn("me", TxnType.DEFAULT);

        NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
        assertEquals(1, rsp.getEventsSize());

        msClient.abortTxns(Collections.singletonList(txnId1));
        rsp = msClient.getNextNotification(firstEventId + 1, 0, null);
        assertEquals(0, rsp.getEventsSize());

        msClient.abortTxns(Collections.singletonList(txnId2));
        rsp = msClient.getNextNotification(firstEventId + 1, 0, null);
        assertEquals(1, rsp.getEventsSize());

        NotificationEvent event = rsp.getEvents().get(0);
        assertEquals(firstEventId + 2, event.getEventId());
        assertTrue(event.getEventTime() >= startTime);
        assertEquals(EventType.ABORT_TXN.toString(), event.getEventType());
    }

    @Test
    public void rollbackTxn() throws Exception {
        long txnId1 = msClient.openTxn("me", TxnType.READ_ONLY);
        long txnId2 = msClient.openTxn("me", TxnType.DEFAULT);

        NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
        assertEquals(1, rsp.getEventsSize());

        msClient.rollbackTxn(txnId1);
        rsp = msClient.getNextNotification(firstEventId + 1, 0, null);
        assertEquals(0, rsp.getEventsSize());

        msClient.rollbackTxn(txnId2);
        rsp = msClient.getNextNotification(firstEventId + 1, 0, null);
        assertEquals(1, rsp.getEventsSize());

        NotificationEvent event = rsp.getEvents().get(0);
        assertEquals(firstEventId + 2, event.getEventId());
        assertTrue(event.getEventTime() >= startTime);
        assertEquals(EventType.ABORT_TXN.toString(), event.getEventType());
    }

    @Test
    public void commitTxn() throws Exception {
        long txnId1 = msClient.openTxn("me", TxnType.READ_ONLY);
        long txnId2 = msClient.openTxn("me", TxnType.DEFAULT);

        NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
        assertEquals(1, rsp.getEventsSize());

        msClient.commitTxn(txnId1);
        rsp = msClient.getNextNotification(firstEventId + 1, 0, null);
        assertEquals(0, rsp.getEventsSize());

        msClient.commitTxn(txnId2);
        rsp = msClient.getNextNotification(firstEventId + 1, 0, null);
        assertEquals(1, rsp.getEventsSize());

        NotificationEvent event = rsp.getEvents().get(0);
        assertEquals(firstEventId + 2, event.getEventId());
        assertTrue(event.getEventTime() >= startTime);
        assertEquals(EventType.COMMIT_TXN.toString(), event.getEventType());
    }

}