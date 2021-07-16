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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.log.syslog;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.log.syslog.SyslogSerDe;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Test suite for the SyslogSerDe class.
 */
public class TestSysLogSerDe {

  @Test
  public void testAllValid() throws Exception {
    SyslogSerDe serde = new SyslogSerDe();
    serde.initialize(null, new Properties(), null);

    Text rowText = new Text(
            "<14>1 2020-05-12T08:48:05.299Z hostname.cluster.local coordinator testProc" +
                    " 92a0ae-833-4fd-8d3-0a4a21ca2 [test@001 class=\"util.Version\"" +
                    " level=\"INFO\" thread=\"main\"] HV000001");

    final List<?> results = (List<?>) serde.deserialize(rowText);

    Assert.assertNotNull(results);
    Assert.assertEquals(10, results.size());
    Assert.assertEquals("USER", results.get(0));
    Assert.assertEquals("INFO", results.get(1));
    Assert.assertEquals("RFC5424", results.get(2));
    Assert.assertEquals("2020-05-12 01:48:05.299",
            ((Timestamp) results.get(3)).toSqlTimestamp().toString());
    Assert.assertEquals("hostname.cluster.local", results.get(4));
    Assert.assertEquals("coordinator", results.get(5));
    Assert.assertEquals("testProc", results.get(6));
    Assert.assertEquals("92a0ae-833-4fd-8d3-0a4a21ca2", results.get(7));
    HashMap map = (HashMap) results.get(8);

    Object key = map.keySet().toArray()[0];
    Assert.assertEquals("level" , ((String)key));
    Assert.assertEquals("INFO" , map.get(key));

    key = map.keySet().toArray()[1];
    Assert.assertEquals("sdId" , ((String)key));
    Assert.assertEquals("test@001" , map.get(key));

    key = map.keySet().toArray()[2];
    Assert.assertEquals("thread" , ((String)key));
    Assert.assertEquals("main" , map.get(key));

    key = map.keySet().toArray()[3];
    Assert.assertEquals("class" , ((String)key));
    Assert.assertEquals("util.Version" , map.get(key));
  }


  @Test
  public void testWithMissingFields() throws Exception {
    SyslogSerDe serde = new SyslogSerDe();
    serde.initialize(null, new Properties(), null);

    Text rowText = new Text(
            "<14>1 2020-05-12T08:48:05.299Z hostname.cluster.local - -" +
                    " - [test@001 class=\"util.Version\"" +
                    " level=\"INFO\" thread=\"main\"] HV000001");

    final List<?> results = (List<?>) serde.deserialize(rowText);

    Assert.assertNotNull(results);
    Assert.assertEquals(10, results.size());
    Assert.assertEquals("USER", results.get(0));
    Assert.assertEquals("INFO", results.get(1));
    Assert.assertEquals("RFC5424", results.get(2));
    Assert.assertEquals("2020-05-12 01:48:05.299",
          ((Timestamp) results.get(3)).toSqlTimestamp().toString());
    Assert.assertEquals("hostname.cluster.local", results.get(4));
    Assert.assertEquals("", results.get(5));
    Assert.assertEquals("", results.get(6));
    Assert.assertEquals("", results.get(7));
    HashMap map = (HashMap) results.get(8);

    Object key = map.keySet().toArray()[0];
    Assert.assertEquals("level" , ((String)key));
    Assert.assertEquals("INFO" , map.get(key));

    key = map.keySet().toArray()[1];
    Assert.assertEquals("sdId" , ((String)key));
    Assert.assertEquals("test@001" , map.get(key));

    key = map.keySet().toArray()[2];
    Assert.assertEquals("thread" , ((String)key));
    Assert.assertEquals("main" , map.get(key));

    key = map.keySet().toArray()[3];
    Assert.assertEquals("class" , ((String)key));
    Assert.assertEquals("util.Version" , map.get(key));
  }
}
