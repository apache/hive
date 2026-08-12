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
package org.apache.hive.hcatalog.mapreduce;

import java.util.Properties;

import org.junit.Assert;

import org.junit.Test;

public class TestInputJobInfo extends HCatBaseTest {

  @Test
  public void test4ArgCreate() throws Exception {
    Properties p = new Properties();
    p.setProperty("key", "value");
    InputJobInfo jobInfo = InputJobInfo.create("Db", "Table", "Filter", p);
    Assert.assertEquals("Db", jobInfo.getDatabaseName());
    Assert.assertEquals("Table", jobInfo.getTableName());
    Assert.assertEquals("Filter", jobInfo.getFilter());
    Assert.assertEquals("value", jobInfo.getProperties().getProperty("key"));
  }

}
