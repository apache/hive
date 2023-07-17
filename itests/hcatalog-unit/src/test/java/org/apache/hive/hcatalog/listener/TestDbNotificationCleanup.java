/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.listener;

import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.REPL_EVENT_DB_LISTENER_TTL;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.EVENT_DB_LISTENER_TTL;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL;
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
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.BatchAcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
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
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Ignore;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;


public class TestDbNotificationCleanup {
    private static final Logger LOG = LoggerFactory.getLogger(TestDbNotificationCleanup.class
            .getName());
    private static final int EVENTS_TTL = 30;
    private static final int CLEANUP_SLEEP_TIME = 10;
    private static Map<String, String> emptyParameters = new HashMap<String, String>();
    private static IMetaStoreClient msClient;
    private static IDriver driver;
    private static MessageDeserializer md;

    private static HiveConf conf;

    static {
        try {
            md = MessageFactory.getInstance(JSONMessageEncoder.FORMAT).getDeserializer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int startTime;
    private long firstEventId;
    private final String testTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "testDbNotif").toString();

    private static List<String> testsToSkipForReplV1BackwardCompatTesting =
            new ArrayList<>(Arrays.asList("cleanupNotifs", "cleanupNotificationWithError","skipCleanedUpEvents"));
    // Make sure we skip backward-compat checking for those tests that don't generate events

    private static ReplicationV1CompatRule bcompat = null;

    @Rule
    public TestRule replV1BackwardCompatibleRule = bcompat;


    @SuppressWarnings("rawtypes")
    @BeforeClass
    public static void connectToMetastore() throws Exception {
        conf = new HiveConf();

        MetastoreConf.setVar(conf,MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS,
                "org.apache.hive.hcatalog.listener.DbNotificationListener");
        conf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL, true);
        conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        conf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
        conf.setBoolVar(HiveConf.ConfVars.REPLCMENABLED, false);
        conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL, DummyRawStoreFailEvent.class.getName());
        MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL, CLEANUP_SLEEP_TIME, TimeUnit.SECONDS);
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY, JSONMessageEncoder.class.getName());
        MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_TTL, EVENTS_TTL, TimeUnit.SECONDS);
        MetastoreConf.setTimeVar(conf, EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL, 20, TimeUnit.SECONDS);

        conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
                "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
        SessionState.start(new CliSessionState(conf));

        msClient = new HiveMetaStoreClient(conf);
        driver = DriverFactory.newDriver(conf);
        md = JSONMessageEncoder.getInstance().getDeserializer();

        bcompat = new ReplicationV1CompatRule(msClient, conf, testsToSkipForReplV1BackwardCompatTesting);
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

    @AfterClass
    public static void tearDownAfterClass() {

        if (msClient != null) {
            msClient.close();
        }
        if (driver != null) {
            driver.close();
        }
        conf = null;
    }

    @After
    public void tearDown() {

    }



    @Test
    public void cleanupNotifs() throws Exception {
        Database db = new Database("cleanup1", "no description", testTempDir, emptyParameters);
        msClient.createDatabase(db);
        msClient.dropDatabase("cleanup1");

        LOG.info("Pulling events immediately after createDatabase/dropDatabase");
        NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
        assertEquals(2, rsp.getEventsSize());

        // sleep for expiry time, and then fetch again
        // sleep twice the TTL interval - things should have been cleaned by then.
        Thread.sleep(EVENTS_TTL * 2 * 1000);

        LOG.info("Pulling events again after cleanup");
        NotificationEventResponse rsp2 = msClient.getNextNotification(firstEventId, 0, null);
        LOG.info("second trigger done");
        assertEquals(0, rsp2.getEventsSize());
    }

    /**
     * Test makes sure that if you use the API {@link HiveMetaStoreClient#getNextNotification(NotificationEventRequest, boolean, NotificationFilter)}
     * does not error out if the events are cleanedup.
     */
    @Test
    public void skipCleanedUpEvents() throws Exception {
        Database db = new Database("cleanup1", "no description", testTempDir, emptyParameters);
        msClient.createDatabase(db);
        msClient.dropDatabase("cleanup1");

        // sleep for expiry time, and then fetch again
        // sleep twice the TTL interval - things should have been cleaned by then.
        Thread.sleep(EVENTS_TTL * 2 * 1000);

        db = new Database("cleanup2", "no description", testTempDir, emptyParameters);
        msClient.createDatabase(db);
        msClient.dropDatabase("cleanup2");

        // the firstEventId is before the cleanup happened, so we should just receive the
        // events which remaining after cleanup.
        NotificationEventRequest request = new NotificationEventRequest();
        request.setLastEvent(firstEventId);
        request.setMaxEvents(-1);
        NotificationEventResponse rsp2 = msClient.getNextNotification(request, true, null);
        assertEquals(2, rsp2.getEventsSize());
        // when we pass the allowGapsInEvents as false the API should error out
        Exception ex = null;
        try {
            NotificationEventResponse rsp = msClient.getNextNotification(request, false, null);
        } catch (Exception e) {
            ex = e;
        }
        assertNotNull(ex);
    }

    @Test
    public void cleanupNotificationWithError() throws Exception {
        Database db = new Database("cleanup1", "no description", testTempDir, emptyParameters);
        msClient.createDatabase(db);
        msClient.dropDatabase("cleanup1");

        LOG.info("Pulling events immediately after createDatabase/dropDatabase");
        NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
        assertEquals(2, rsp.getEventsSize());
        //this simulates that cleaning thread will error out while cleaning the notifications
        DummyRawStoreFailEvent.setEventSucceed(false);
        // sleep for expiry time, and then fetch again
        // sleep twice the TTL interval - things should have been cleaned by then.
        Thread.sleep(EVENTS_TTL * 2 * 1000);

        LOG.info("Pulling events again after failing to cleanup");
        NotificationEventResponse rsp2 = msClient.getNextNotification(firstEventId, 0, null);
        LOG.info("second trigger done");
        assertEquals(2, rsp2.getEventsSize());
        DummyRawStoreFailEvent.setEventSucceed(true);
        Thread.sleep(EVENTS_TTL * 2 * 1000);

        LOG.info("Pulling events again after cleanup");
        rsp2 = msClient.getNextNotification(firstEventId, 0, null);
        LOG.info("third trigger done");
        assertEquals(0, rsp2.getEventsSize());
    }
}


