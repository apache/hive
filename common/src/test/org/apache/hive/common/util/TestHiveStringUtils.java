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

package org.apache.hive.common.util;

import static org.apache.hive.common.util.HiveStringUtils.removeComments;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

public class TestHiveStringUtils {
  @Test
  public void testSplitAndUnEscape() throws Exception {
    splitAndUnEscapeTestCase(
        null, null);

    splitAndUnEscapeTestCase(
        "'single element'",
        new String[] {
            "'single element'"
        });

    splitAndUnEscapeTestCase(
        "yyyy-MM-dd'T'HH:mm:ss,yyyy-MM-dd'T'HH:mm:ss.S",
        new String[] {
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss.S"
        });

    splitAndUnEscapeTestCase(
        "single\\,element",
        new String[] {
            "single,element"
        });
    splitAndUnEscapeTestCase(
        "element\\,one\\\\,element\\\\two\\\\\\,",
        new String[] {
            "element,one\\",
            "element\\two\\,"
        });
  }

  public void splitAndUnEscapeTestCase(String testValue, String[] expectedResults) throws Exception {
    String[] testResults = HiveStringUtils.splitAndUnEscape(testValue);
    assertTrue(Arrays.toString(expectedResults) + " == " + Arrays.toString(testResults),
        Arrays.equals(expectedResults, testResults));
  }

  @Test
  public void testStripComments() throws Exception {
    assertNull(removeComments(null));
    assertUnchanged("foo");
    assertUnchanged("select 1");
    assertUnchanged("insert into foo (values('-----')");
    assertUnchanged("insert into foo (values('abc\n\'xyz')");
    assertUnchanged("create database if not exists testDB; set hive.cli.print.current.db=true;use\ntestDB;\nuse default;drop if exists testDB;");

    assertEquals("foo", removeComments("foo\n"));
    assertEquals("foo", removeComments("\nfoo"));
    assertEquals("foo", removeComments("\n\nfoo\n\n"));
    assertEquals("insert into foo (values('-----')", removeComments("--comment\ninsert into foo (values('-----')"));
    assertEquals("insert into foo (values('----''-')", removeComments("--comment\ninsert into foo (values('----''-')"));
    assertEquals("insert into foo (values(\"----''-\")", removeComments("--comment\ninsert into foo (values(\"----''-\")"));
    assertEquals("insert into foo (values(\"----\"\"-\")", removeComments("--comment\ninsert into foo (values(\"----\"\"-\")"));
    assertEquals("insert into foo (values('-\n--\n--')", removeComments("--comment\ninsert into foo (values('-\n--\n--')"));
    assertEquals("insert into foo (values('-\n--\n--')", removeComments("--comment\n\ninsert into foo (values('-\n--\n--')"));
    assertEquals("insert into foo (values(\"-\n--\n--\")", removeComments("--comment\n\ninsert into foo (values(\"-\n--\n--\")"));
    assertEquals("insert into foo (values(\"-\n--\n--\")", removeComments("\n\n--comment\n\ninsert into foo (values(\"-\n--\n--\")\n\n"));
    assertEquals("insert into foo (values('abc');\ninsert into foo (values('def');", removeComments( "insert into foo (values('abc');\n--comment\ninsert into foo (values('def');"));
  }

  @Test
  public void testLinesEndingWithComments() {
    int[] escape = {-1};
    assertEquals("show tables;", removeComments("show tables;",escape));
    assertEquals("show tables; ", removeComments("show tables; --comments",escape));
    assertEquals("show tables; ", removeComments("show tables; -------comments",escape));
    assertEquals("show tables; ", removeComments("show tables; -------comments;one;two;three;;;;",escape));
    assertEquals("show", removeComments("show-- tables; -------comments",escape));
    assertEquals("show ", removeComments("show --tables; -------comments",escape));
    assertEquals("s", removeComments("s--how --tables; -------comments",escape));
    assertEquals("", removeComments("-- show tables; -------comments",escape));

    assertEquals("\"show tables\" ", removeComments("\"show tables\" --comments",escape));
    assertEquals("\"show --comments tables\" ", removeComments("\"show --comments tables\" --comments",escape));
    assertEquals("\"'show --comments' tables\" ", removeComments("\"'show --comments' tables\" --comments",escape));
    assertEquals("'show --comments tables' ", removeComments("'show --comments tables' --comments",escape));
    assertEquals("'\"show --comments tables\"' ", removeComments("'\"show --comments tables\"' --comments",escape));
  }

  /**
   * check that statement is unchanged after stripping
   */
  private void assertUnchanged(String statement) {
    assertEquals("statement should not have been affected by stripping comments", statement,
        removeComments(statement));
  }
}
