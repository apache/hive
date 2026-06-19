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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.Constants;
import org.junit.Assert;
import org.junit.Test;

public class TestServiceContext {

  @Test
  public void testInit() {
    ServiceContext serviceContext = new ServiceContext(() -> "testhost", () -> 1234);
    Assert.assertEquals("testhost", serviceContext.getHost());
    Assert.assertEquals(1234, serviceContext.getPort());
  }

  @Test
  public void testClusterId() {
    // 1. find cli arg value (defined in root pom.xml)
    ServiceContext serviceContext = new ServiceContext(null, null);
    Assert.assertEquals("hive-test-cluster-id-cli", serviceContext.getClusterId());

    // 2. if cli arg value is not present, find in env (defined in root pom.xml)
    System.getProperties().remove(Constants.CLUSTER_ID_CLI_OPT_NAME);
    serviceContext = new ServiceContext(null, null);
    Assert.assertEquals("hive-test-cluster-id-env", serviceContext.getClusterId());
  }
}
