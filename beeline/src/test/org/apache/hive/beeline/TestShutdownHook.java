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

package org.apache.hive.beeline;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestShutdownHook {
  @Test
  public void testShutdownHook() throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ops = new PrintStream(os);
    BeeLine beeline = new BeeLine();
    DatabaseConnections dbConnections = beeline.getDatabaseConnections();
    dbConnections.setConnection(new DatabaseConnection(beeline,null,null, null));
    dbConnections.setConnection(new DatabaseConnection(beeline,null,null, null));
    Assert.assertEquals(2, dbConnections.size());
    beeline.setOutputStream(ops);
    beeline.getShutdownHook().run();
    Assert.assertEquals(0, dbConnections.size());
  }
}