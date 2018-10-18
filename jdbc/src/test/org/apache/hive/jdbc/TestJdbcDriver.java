/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestJdbcDriver {
  private static File file = null;
  private String input;
  private String expected;

  public TestJdbcDriver(String input, String expected) throws Exception {
    this.input = input;
    this.expected = expected;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
      // Here are some positive cases which can be executed as below :
      {"show databases;show tables;", "show databases,show tables"},
      {" show\n\r  tables;", "show tables"},
      {"show databases; show\ntables;", "show databases,show tables"},
      {"show    tables;", "show    tables"},
      {"show tables ;", "show tables"},
      // Here are some negative cases as below :
      {"show tables", ","},
      {"show tables show tables;", "show tables show tables"},
      {"show tab les;", "show tab les"},
      {"#show tables; show\n tables;", "tables"},
      {"show tab les;show tables;", "show tab les,show tables"}
    });
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    file = new File(System.getProperty("user.dir") + File.separator + "Init.sql");
    if (!file.exists()) {
      file.createNewFile();
    }
  }

  @AfterClass
  public static void cleanUpAfterClass() throws Exception {
    if (file != null) {
      file.delete();
    }
  }

  @Test
  public void testParseInitFile() throws IOException {
    BufferedWriter bw = null;
    try {
      bw = new BufferedWriter(new FileWriter(file));
      bw.write(input);
      bw.flush();
      assertEquals(Arrays.asList(expected.split(",")), HiveConnection.parseInitFile(file.toString()));
    } catch(Exception e) {
      Assert.fail("Test was failed due to " + e);
    } finally {
      if (bw != null) {
        bw.close();
      }
    }
  }
}
