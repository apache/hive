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
package org.apache.hadoop.hive.ql.parse;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Parser tests empty text in quoted identifiers.
 */
public class TestEmptyQuotedIdentifiersStandardNegative {
  private static Configuration conf;

  private ParseDriver parseDriver;

  @BeforeClass
  public static void initialize() {
    conf = new Configuration();
    conf.set("hive.support.quoted.identifiers", "standard");
  }

  @Before
  public void setup() throws SemanticException, IOException {
    parseDriver = new ParseDriver();
  }

  private void test(String query, String expectedErrorMessage) {
    try {
      parseDriver.parse(query, conf);
      Assert.fail("Expected ParseException");
    } catch (ParseException e) {
      Assert.assertEquals(expectedErrorMessage, e.getMessage());
    }

  }

  @Test
  public void testTableName() {
    test(
        "select * from \"test_database\".\"\"",
        "line 1:32 cannot recognize input near 'test_database' '.' '<EOF>' in table name"
    );
    test(
        "select * from \"\"",
        "line 1:16 cannot recognize input near '<EOF>' '<EOF>' '<EOF>' in join source"
    );
  }

  @Test
  public void testTableNameWithBacktick() {
    test(
        "select * from `test_database`.``",
        "line 1:32 cannot recognize input near 'test_database' '.' '<EOF>' in table name"
    );
    test(
        "select * from ``",
        "line 1:16 cannot recognize input near '<EOF>' '<EOF>' '<EOF>' in join source"
    );
  }

  @Test
  public void testColumnName() {
    test(
        "select \"\" from \"test_database\".\"test_table\"",
        "line 1:10 cannot recognize input near 'from' 'test_database' '.' in select clause"
    );
  }

  @Test
  public void testColumnNameWithBacktick() {
    test(
        "select `` from `test_database`.`test_table`",
        "line 1:10 cannot recognize input near 'from' 'test_database' '.' in select clause"
    );
  }
}
