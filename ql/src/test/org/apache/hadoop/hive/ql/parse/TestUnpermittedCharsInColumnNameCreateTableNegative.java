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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Parser tests for unpermitted chars in column names. Please find more
 * information in HIVE-10120
 */
public class TestUnpermittedCharsInColumnNameCreateTableNegative {
  private static HiveConf conf;

  private ParseDriver pd;

  @BeforeClass
  public static void initialize() {
    conf = new HiveConf(SemanticAnalyzer.class);
    SessionState.start(conf);
  }

  @Before
  public void setup() throws SemanticException, IOException {
    pd = new ParseDriver();
  }

  ASTNode parse(String query) throws ParseException {
    ASTNode nd = null;
    try {
      nd = pd.parse(query, new Context(conf));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return (ASTNode) nd.getChild(0);
  }

  @Test
  public void testDotInCreateTable() {
    try {
      parse("CREATE TABLE testTable (`emp.no` STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:39 Failed to recognize predicate ')'. Failed rule: '[., :] can not be used in column name in create table statement.' in column specification",
              ex.getMessage());
    }
  }

  @Test
  public void testColonInCreateTable() {
    try {
      parse("CREATE TABLE testTable (`emp:no` STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:39 Failed to recognize predicate ')'. Failed rule: '[., :] can not be used in column name in create table statement.' in column specification",
              ex.getMessage());
    }
  }
}
