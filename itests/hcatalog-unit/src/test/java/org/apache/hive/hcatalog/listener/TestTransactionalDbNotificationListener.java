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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Collections;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;



public class TestTransactionalDbNotificationListener {
    private static IMetaStoreClient msClient;

    private int startTime;
    private long firstEventId;


    @BeforeClass
    public static void connectToMetastore() throws Exception {
        HiveConf conf = new HiveConfForTest(TestTransactionalDbNotificationListener.class);
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