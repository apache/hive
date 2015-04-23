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
package org.apache.hive.hcatalog.api.repl;

import junit.framework.TestCase;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatNotificationEvent;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.messaging.MessageFactory;
import org.junit.Test;

public class TestReplicationTask extends TestCase{
  private static MessageFactory msgFactory = MessageFactory.getInstance();


  @Test
  public static void testCreate() throws HCatException {
    Table t = new Table();
    t.setDbName("testdb");
    t.setTableName("testtable");
    NotificationEvent event = new NotificationEvent(0, (int)System.currentTimeMillis(),
        HCatConstants.HCAT_CREATE_TABLE_EVENT, msgFactory.buildCreateTableMessage(t).toString());
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());

    ReplicationTask.resetFactory(null);
    ReplicationTask rtask = ReplicationTask.create(HCatClient.create(new HiveConf()),new HCatNotificationEvent(event));

    assertTrue("Default factory instantiation should yield NoopReplicationTask", rtask instanceof NoopReplicationTask);
  }

}
