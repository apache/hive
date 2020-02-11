/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.api.server;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;

import org.junit.Assert;

import com.google.common.base.Charsets;

public class TestTestLogger {

  @org.junit.Test
  public void basicTest() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream logFile = new PrintStream(out);
    TestLogger logger = new TestLogger(logFile, TestLogger.LEVEL.INFO);
    logger.trace("trace");
    logger.debug("debug");
    logger.info("info");
    logger.warn("warn");
    logger.error("error");
    logFile.flush();
    logFile.close();
    BufferedReader reader = new BufferedReader(new StringReader(new String(out.toByteArray(), Charsets.UTF_8)));
    String info = reader.readLine();
    String warn = reader.readLine();
    String error = reader.readLine();
    Assert.assertNull(reader.readLine());
    Assert.assertTrue(info, info.endsWith("info"));
    Assert.assertTrue(warn, warn.endsWith("warn"));
    Assert.assertTrue(error, error.endsWith("error"));
  }
}
