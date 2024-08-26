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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.util.concurrent.TimeUnit;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL;



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

    private long firstEventId;
    private final String testTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "testDbNotif").toString();
    @SuppressWarnings("rawtypes")
    @BeforeClass
    public static void connectToMetastore() throws Exception {
        conf = new HiveConf();
        //TODO: HIVE-27998: hcatalog tests on Tez
        conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
        MetastoreConf.setVar(conf,MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS,
                "org.apache.hive.hcatalog.listener.DbNotificationListener");
        conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        conf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
        conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL, DummyRawStoreFailEvent.class.getName());
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY, JSONMessageEncoder.class.getName());
        MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL, CLEANUP_SLEEP_TIME, TimeUnit.SECONDS);
        MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_TTL, EVENTS_TTL, TimeUnit.SECONDS);
        MetastoreConf.setTimeVar(conf, EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL, 20, TimeUnit.SECONDS);
        conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
                "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
        SessionState.start(new CliSessionState(conf));
        msClient = new HiveMetaStoreClient(conf);
        driver = DriverFactory.newDriver(conf);

    }

    @Before
    public void setup() throws Exception {
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


