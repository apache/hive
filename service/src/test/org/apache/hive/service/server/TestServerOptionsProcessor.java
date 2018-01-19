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

package org.apache.hive.service.server;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hive.service.server.HiveServer2.ServerOptionsProcessor;

/**
 * Test ServerOptionsProcessor
 *
 */
public class TestServerOptionsProcessor {

  @Test
  public void test() {
    ServerOptionsProcessor optProcessor = new ServerOptionsProcessor("HiveServer2");
    final String key = "testkey";
    final String value = "value123";
    String []args = {"-hiveconf", key + "=" + value};

    Assert.assertEquals(
        "checking system property before processing options",
        null,
        System.getProperty(key));

    optProcessor.parse(args);

    Assert.assertEquals(
        "checking system property after processing options",
        value,
        System.getProperty(key));
  }

}
