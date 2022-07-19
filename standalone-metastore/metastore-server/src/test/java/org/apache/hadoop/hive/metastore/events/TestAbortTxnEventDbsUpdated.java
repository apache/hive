/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Category(MetastoreUnitTest.class)
public class TestAbortTxnEventDbsUpdated {
  @Test
  public void testBackwardsCompatibility() {
    final String json = "{\"txnid\":12787,\"timestamp\":1654116516,\"server\":\"\",\"servicePrincipal\":\"\"}";
    JSONMessageDeserializer deserializer = new JSONMessageDeserializer();
    AbortTxnMessage abortTxnMsg = deserializer.getAbortTxnMessage(json);
    Assert.assertNull(abortTxnMsg.getDbsUpdated());
    Assert.assertEquals(12787L, abortTxnMsg.getTxnId().longValue());
  }

  @Test
  public void testSerializeDeserialize() {
    List dbsUpdated = Arrays.asList("db1", "db22");
    AbortTxnEvent event = new AbortTxnEvent(999L, TxnType.DEFAULT, null, dbsUpdated);
    AbortTxnMessage msg =
            MessageBuilder.getInstance().buildAbortTxnMessage(event.getTxnId(), event.getDbsUpdated());
    JSONMessageEncoder msgEncoder = new JSONMessageEncoder();
    String json = msgEncoder.getSerializer().serialize(msg);

    JSONMessageDeserializer deserializer = new JSONMessageDeserializer();
    AbortTxnMessage abortTxnMsg = deserializer.getAbortTxnMessage(json);
    Set<String> expected = new HashSet(dbsUpdated);
    Assert.assertEquals(expected.size(), abortTxnMsg.getDbsUpdated().size());
    List actual = abortTxnMsg.getDbsUpdated();
    Assert.assertTrue(actual.remove("db1"));
    Assert.assertTrue(actual.remove("db22"));
    Assert.assertTrue(actual.isEmpty());
    Assert.assertEquals(999L, abortTxnMsg.getTxnId().longValue());
  }
}
