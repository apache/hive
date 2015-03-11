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
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Parser tests for SQL11 Reserved KeyWords. Please find more information in
 * HIVE-6617. Total number : 74
 */
public class TestSQL11ReservedKeyWordsNegative {
  private static HiveConf conf;

  private ParseDriver pd;

  @BeforeClass
  public static void initialize() {
    conf = new HiveConf(SemanticAnalyzer.class);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS, true);
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
  public void testSQL11ReservedKeyWords_ALL() {
    try {
      parse("CREATE TABLE ALL (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'ALL'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_ALTER() {
    try {
      parse("CREATE TABLE ALTER (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'ALTER'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_ARRAY() {
    try {
      parse("CREATE TABLE ARRAY (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'ARRAY'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_AS() {
    try {
      parse("CREATE TABLE AS (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'AS'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_AUTHORIZATION() {
    try {
      parse("CREATE TABLE AUTHORIZATION (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'AUTHORIZATION'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_BETWEEN() {
    try {
      parse("CREATE TABLE BETWEEN (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'BETWEEN'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_BIGINT() {
    try {
      parse("CREATE TABLE BIGINT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'BIGINT'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_BINARY() {
    try {
      parse("CREATE TABLE BINARY (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'BINARY'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_BOOLEAN() {
    try {
      parse("CREATE TABLE BOOLEAN (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'BOOLEAN'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_BOTH() {
    try {
      parse("CREATE TABLE BOTH (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'BOTH'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_BY() {
    try {
      parse("CREATE TABLE BY (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'BY'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_CREATE() {
    try {
      parse("CREATE TABLE CREATE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'CREATE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_CUBE() {
    try {
      parse("CREATE TABLE CUBE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'CUBE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_CURRENT_DATE() {
    try {
      parse("CREATE TABLE CURRENT_DATE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'CURRENT_DATE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_CURRENT_TIMESTAMP() {
    try {
      parse("CREATE TABLE CURRENT_TIMESTAMP (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'CURRENT_TIMESTAMP'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_CURSOR() {
    try {
      parse("CREATE TABLE CURSOR (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'CURSOR'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_DATE() {
    try {
      parse("CREATE TABLE DATE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'DATE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_DECIMAL() {
    try {
      parse("CREATE TABLE DECIMAL (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'DECIMAL'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_DELETE() {
    try {
      parse("CREATE TABLE DELETE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'DELETE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_DESCRIBE() {
    try {
      parse("CREATE TABLE DESCRIBE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'DESCRIBE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_DOUBLE() {
    try {
      parse("CREATE TABLE DOUBLE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'DOUBLE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_DROP() {
    try {
      parse("CREATE TABLE DROP (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'DROP'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_EXISTS() {
    try {
      parse("CREATE TABLE EXISTS (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'EXISTS'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_EXTERNAL() {
    try {
      parse("CREATE TABLE EXTERNAL (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'EXTERNAL'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_FALSE() {
    try {
      parse("CREATE TABLE FALSE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'FALSE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_FETCH() {
    try {
      parse("CREATE TABLE FETCH (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'FETCH'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_FLOAT() {
    try {
      parse("CREATE TABLE FLOAT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'FLOAT'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_FOR() {
    try {
      parse("CREATE TABLE FOR (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'FOR'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_FULL() {
    try {
      parse("CREATE TABLE FULL (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'FULL'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_GRANT() {
    try {
      parse("CREATE TABLE GRANT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'GRANT'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_GROUP() {
    try {
      parse("CREATE TABLE GROUP (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'GROUP'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_GROUPING() {
    try {
      parse("CREATE TABLE GROUPING (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'GROUPING'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_IMPORT() {
    try {
      parse("CREATE TABLE IMPORT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'IMPORT'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_IN() {
    try {
      parse("CREATE TABLE IN (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'IN'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_INNER() {
    try {
      parse("CREATE TABLE INNER (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'INNER'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_INSERT() {
    try {
      parse("CREATE TABLE INSERT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'INSERT'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_INT() {
    try {
      parse("CREATE TABLE INT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'INT'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_INTERSECT() {
    try {
      parse("CREATE TABLE INTERSECT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'INTERSECT'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_INTO() {
    try {
      parse("CREATE TABLE INTO (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'INTO'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_IS() {
    try {
      parse("CREATE TABLE IS (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'IS'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_LATERAL() {
    try {
      parse("CREATE TABLE LATERAL (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'LATERAL'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_LEFT() {
    try {
      parse("CREATE TABLE LEFT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'LEFT'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_LIKE() {
    try {
      parse("CREATE TABLE LIKE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'LIKE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_LOCAL() {
    try {
      parse("CREATE TABLE LOCAL (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'LOCAL'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_NONE() {
    try {
      parse("CREATE TABLE NONE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'NONE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_NULL() {
    try {
      parse("CREATE TABLE NULL (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'NULL'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_OF() {
    try {
      parse("CREATE TABLE OF (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'OF'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_ORDER() {
    try {
      parse("CREATE TABLE ORDER (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'ORDER'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_OUT() {
    try {
      parse("CREATE TABLE OUT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'OUT'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_OUTER() {
    try {
      parse("CREATE TABLE OUTER (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'OUTER'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_PARTITION() {
    try {
      parse("CREATE TABLE PARTITION (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'PARTITION'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_PERCENT() {
    try {
      parse("CREATE TABLE PERCENT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'PERCENT'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_PROCEDURE() {
    try {
      parse("CREATE TABLE PROCEDURE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'PROCEDURE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_RANGE() {
    try {
      parse("CREATE TABLE RANGE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'RANGE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_READS() {
    try {
      parse("CREATE TABLE READS (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'READS'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_REVOKE() {
    try {
      parse("CREATE TABLE REVOKE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'REVOKE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_RIGHT() {
    try {
      parse("CREATE TABLE RIGHT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'RIGHT'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_ROLLUP() {
    try {
      parse("CREATE TABLE ROLLUP (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'ROLLUP'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_ROW() {
    try {
      parse("CREATE TABLE ROW (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'ROW'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_ROWS() {
    try {
      parse("CREATE TABLE ROWS (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'ROWS'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_SET() {
    try {
      parse("CREATE TABLE SET (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'SET'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_SMALLINT() {
    try {
      parse("CREATE TABLE SMALLINT (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'SMALLINT'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_TABLE() {
    try {
      parse("CREATE TABLE TABLE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'TABLE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_TIMESTAMP() {
    try {
      parse("CREATE TABLE TIMESTAMP (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'TIMESTAMP'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_TO() {
    try {
      parse("CREATE TABLE TO (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:13 Failed to recognize predicate 'TO'. Failed rule: 'identifier' in table name",
          ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_TRIGGER() {
    try {
      parse("CREATE TABLE TRIGGER (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'TRIGGER'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_TRUE() {
    try {
      parse("CREATE TABLE TRUE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'TRUE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_TRUNCATE() {
    try {
      parse("CREATE TABLE TRUNCATE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'TRUNCATE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_UNION() {
    try {
      parse("CREATE TABLE UNION (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'UNION'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_UPDATE() {
    try {
      parse("CREATE TABLE UPDATE (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'UPDATE'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_USER() {
    try {
      parse("CREATE TABLE USER (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'USER'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_USING() {
    try {
      parse("CREATE TABLE USING (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'USING'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_VALUES() {
    try {
      parse("CREATE TABLE VALUES (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'VALUES'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }

  @Test
  public void testSQL11ReservedKeyWords_WITH() {
    try {
      parse("CREATE TABLE WITH (col STRING)");
      Assert.assertFalse("Expected ParseException", true);
    } catch (ParseException ex) {
      Assert
          .assertEquals(
              "Failure didn't match.",
              "line 1:13 Failed to recognize predicate 'WITH'. Failed rule: 'identifier' in table name",
              ex.getMessage());
    }
  }
}
