/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.beeline;

import static org.apache.hive.common.util.HiveStringUtils.removeComments;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

public class TestCommands {

  @Test
  public void testLinesEndingWithComments() {
    assertEquals("show tables;", removeComments("show tables;"));
    assertEquals("show tables;", removeComments("show tables; --comments"));
    assertEquals("show tables;", removeComments("show tables; -------comments"));
    assertEquals("show tables;", removeComments("show tables; -------comments;one;two;three;;;;"));
    assertEquals("show", removeComments("show-- tables; -------comments"));
    assertEquals("show", removeComments("show --tables; -------comments"));
    assertEquals("s", removeComments("s--how --tables; -------comments"));
    assertEquals("", removeComments("-- show tables; -------comments"));

    assertEquals("\"show tables\"", removeComments("\"show tables\" --comments"));
    assertEquals("\"show --comments tables\"", removeComments("\"show --comments tables\" --comments"));
    assertEquals("\"'show --comments' tables\"", removeComments("\"'show --comments' tables\" --comments"));
    assertEquals("'show --comments tables'", removeComments("'show --comments tables' --comments"));
    assertEquals("'\"show --comments tables\"'", removeComments("'\"show --comments tables\"' --comments"));

    assertEquals("show tables;", removeComments("--comments\nshow tables;"));
    assertEquals("show tables;", removeComments("--comments\nshow tables; --comments"));
    assertEquals("show tables;", removeComments("--comments\nshow tables; -------comments"));
    assertEquals("show tables;", removeComments("--comments\nshow tables; -------comments;one;two;three;;;;"));
    assertEquals("show", removeComments("--comments\nshow-- tables; -------comments"));
    assertEquals("show", removeComments("--comments\nshow --tables; -------comments"));
    assertEquals("s", removeComments("--comments\ns--how --tables; -------comments"));
    assertEquals("", removeComments("--comments\n-- show tables; -------comments"));

    assertEquals("\"show tables\"", removeComments("--comments\n\"show tables\" --comments"));
    assertEquals("\"show --comments tables\"", removeComments("--comments\n\"show --comments tables\" --comments"));
    assertEquals("\"'show --comments' tables\"", removeComments("--comments\n\"'show --comments' tables\" --comments"));
    assertEquals("'show --comments tables'", removeComments("--comments\n'show --comments tables' --comments"));
    assertEquals("'\"show --comments tables\"'", removeComments("--comments\n'\"show --comments tables\"' --comments"));
    assertEquals( "select col1, \n" +
                  "       year,\n" +
                  "       month,\n" +
                  "       date\n" +
                  "  from test_table\n" +
                  " where\n" +
                  "   username = 'foo';",
        removeComments("select col1, -- comments\n" +
                       "       --partitioned year column\n" +
                       "       year,\n" +
                       "       --partitioned month column\n" +
                       "       month,\n" +
                       "       --partitioned date column\n" +
                       "       date\n" +
                       "  from test_table\n" +
                       " where\n" +
                       "   --for a particular user\n" +
                       "   username = 'foo';"));
  }

  /**
   * Test the commands directly call from beeline.
   * @throws IOException
   */
  @Test
  public void testBeelineCommands() throws IOException {
 // avoid System.exit() call in beeline which causes JVM to exit and fails the test
    System.setProperty(BeeLineOpts.PROPERTY_NAME_EXIT, "true");
    // Verify the command without ';' at the end also works fine
    BeeLine.mainWithInputRedirection(new String[] {"-u", "jdbc:hive2://", "-e", "select 3"}, null);
    BeeLine.mainWithInputRedirection(
        new String[] {"-u", "jdbc:hive2://", "-e", "create table t1(x int); show tables"}, null);
  }

  /**
   * Test {@link Commands#getCmdList(String, boolean)} with various nesting of special characters:
   * apostrophe, quotation mark, newline, comment start, semicolon.
   * @throws Exception
   */
  @Test
  public void testGetCmdList() throws Exception {
    BeeLine beeline = new BeeLine();
    Commands commands = new Commands(beeline);

    try {
      // COMMANDS, WHITE SPACES

      // trivial
      assertEquals(
          Arrays.asList(""),
          commands.getCmdList("", false)
      );
      assertEquals(
          Arrays.asList(""),
          commands.getCmdList(";", false)
      );
      assertEquals(
          Arrays.asList(" "),
          commands.getCmdList(" ;", false)
      );
      assertEquals(
          Arrays.asList("", " "),
          commands.getCmdList("; ", false)
      );
      assertEquals(
          Arrays.asList(" ", " "),
          commands.getCmdList(" ; ", false)
      );
      assertEquals(
          Arrays.asList(" ; "),
          commands.getCmdList(" \\; ", false)
      );
      assertEquals(
          Arrays.asList("select 1"),
          commands.getCmdList("select 1;", false)
      );
      assertEquals(
          Arrays.asList("select 1"),
          commands.getCmdList("select 1", false)
      );
      // add whitespace
      assertEquals(
          Arrays.asList(" \n select \n 1 \n "),
          commands.getCmdList(" \n select \n 1 \n ;", false)
      );
      // add whitespace after semicolon
      assertEquals(
          Arrays.asList(" \n select 1 \n ", " \n "),
          commands.getCmdList(" \n select 1 \n ; \n ", false)
      );
      // second command
      assertEquals(
          Arrays.asList("select 1", "select 2"),
          commands.getCmdList("select 1;select 2;", false)
      );
      // second command, no ending semicolon
      assertEquals(
          Arrays.asList("select 1", "select 2"),
          commands.getCmdList("select 1;select 2", false)
      );
      // three commands with whitespaces
      assertEquals(
          Arrays.asList(" \n select \t 1", "\tselect\n2\r", " select\n3", "   "),
          commands.getCmdList(" \n select \t 1;\tselect\n2\r; select\n3;   ", false)
      );

      // ADD STRINGS

      // trivial string
      assertEquals(
          Arrays.asList("select 'foo'"),
          commands.getCmdList("select 'foo';", false)
      );
      assertEquals(
          Arrays.asList("select \"foo\""),
          commands.getCmdList("select \"foo\";", false)
      );
      assertEquals(
          Arrays.asList("select 'foo'", " select 2"),
          commands.getCmdList("select 'foo'; select 2;", false)
      );
      assertEquals(
          Arrays.asList("select \"foo\"", " select 2"),
          commands.getCmdList("select \"foo\"; select 2", false)
      );
      assertEquals(
          Arrays.asList("select ''", " select \"\""),
          commands.getCmdList("select ''; select \"\"", false)
      );
      // string containing delimiter of other string
      assertEquals(
          Arrays.asList("select 'foo\"bar'"),
          commands.getCmdList("select 'foo\"bar';", false)
      );
      assertEquals(
          Arrays.asList("select \"foo'bar\""),
          commands.getCmdList("select \"foo'bar\";", false)
      );
      assertEquals(
          Arrays.asList("select 'foo\"bar'", " select 'foo\"bar'"),
          commands.getCmdList("select 'foo\"bar'; select 'foo\"bar';", false)
      );
      assertEquals(
          Arrays.asList("select \"foo'bar\"", " select \"foo'bar\""),
          commands.getCmdList("select \"foo'bar\"; select \"foo'bar\"", false)
      );
      assertEquals(
          Arrays.asList("select '\"' ", " select \"'\" "),
          commands.getCmdList("select '\"' ; select \"'\" ;", false)
      );
      // string containing semicolon
      assertEquals(
          Arrays.asList("select 'foo;bar'"),
          commands.getCmdList("select 'foo;bar';", false)
      );
      assertEquals(
          Arrays.asList("select \"foo;bar\""),
          commands.getCmdList("select \"foo;bar\";", false)
      );
      // two selects of strings vs. one select containing semicolon
      assertEquals(
          Arrays.asList("select '\"foobar'", " select 'foobar\"'"),
          commands.getCmdList("select '\"foobar'; select 'foobar\"';", false)
      );
      assertEquals(
          Arrays.asList("select \"'foobar'; select 'foobar'\""),
          commands.getCmdList("select \"'foobar'; select 'foobar'\";", false)
      );
      // newline within strings
      assertEquals(
          Arrays.asList("select 'multi\nline\nstring'", " select 'allowed'"),
          commands.getCmdList("select 'multi\nline\nstring'; select 'allowed';", false)
      );
      assertEquals(
          Arrays.asList("select \"multi\nline\nstring\"", " select \"allowed\""),
          commands.getCmdList("select \"multi\nline\nstring\"; select \"allowed\";", false)
      );
      assertEquals(
          Arrays.asList("select ';\nselect 1;\n'", " select 'sql within string'"),
          commands.getCmdList("select ';\nselect 1;\n'; select 'sql within string';", false)
      );
      // escaped quotation marks in strings
      assertEquals(
          Arrays.asList("select 'fo\\'o'"),
          commands.getCmdList("select 'fo\\'o';", false)
      );
      assertEquals(
          Arrays.asList("select \"fo\\\"o\""),
          commands.getCmdList("select \"fo\\\"o\";", false)
      );
      assertEquals(
          Arrays.asList("select 'fo\\\"o'"),
          commands.getCmdList("select 'fo\\\"o';", false)
      );
      assertEquals(
          Arrays.asList("select \"fo\\'o\""),
          commands.getCmdList("select \"fo\\'o\";", false)
      );
      // strings ending with backslash
      assertEquals(
          Arrays.asList("select 'foo\\\\'", " select \"bar\\\\\""),
          commands.getCmdList("select 'foo\\\\'; select \"bar\\\\\";", false)
      );

      // ADD LINE COMMENTS

      // line comments
      assertEquals(
          Arrays.asList("select 1", " -- comment\nselect 2", " -- comment\n"),
          commands.getCmdList("select 1; -- comment\nselect 2; -- comment\n", false)
      );
      assertEquals(
          Arrays.asList("select -- comment\n1", " select -- comment\n2"),
          commands.getCmdList("select -- comment\n1; select -- comment\n2;", false)
      );
      assertEquals(
          Arrays.asList("select -- comment 1; select -- comment 2;"),
          commands.getCmdList("select -- comment 1; select -- comment 2;", false)
      );
      assertEquals(
          Arrays.asList("select -- comment\\\n1", " select -- comment\\\n2"),
          commands.getCmdList("select -- comment\\\n1; select -- comment\\\n2;", false)
      );
      // line comments with semicolons
      assertEquals(
          Arrays.asList("select 1 -- invalid;\nselect 2"),
          commands.getCmdList("select 1 -- invalid;\nselect 2;", false)
      );
      assertEquals(
          Arrays.asList("select 1 -- valid\n", "select 2"),
          commands.getCmdList("select 1 -- valid\n;select 2;", false)
      );
      // line comments with quotation marks
      assertEquals(
          Arrays.asList("select 1 -- v'lid\n", "select 2", "select 3"),
          commands.getCmdList("select 1 -- v'lid\n;select 2;select 3;", false)
      );
      assertEquals(
          Arrays.asList("select 1 -- v\"lid\n", "select 2", "select 3"),
          commands.getCmdList("select 1 -- v\"lid\n;select 2;select 3;", false)
      );
      assertEquals(
          Arrays.asList("", "select 1 -- '\n", "select \"'\"", "select 3 -- \"\n", "?"),
          commands.getCmdList(";select 1 -- '\n;select \"'\";select 3 -- \"\n;?", false)
      );
      assertEquals(
          Arrays.asList("", "select 1 -- ';select \"'\"\n", "select 3 -- \"\n", "?"),
          commands.getCmdList(";select 1 -- ';select \"'\"\n;select 3 -- \"\n;?", false)
      );

      // ADD BLOCK COMMENTS

      // block comments with semicolons
      assertEquals(
          Arrays.asList("select 1", " select /* */ 2", " select /* */ 3"),
          commands.getCmdList("select 1; select /* */ 2; select /* */ 3;", false)
      );
      assertEquals(
          Arrays.asList("select 1", " select /* ; */ 2", " select /* ; */ 3"),
          commands.getCmdList("select 1; select /* ; */ 2; select /* ; */ 3;", false)
      );
      assertEquals(
          Arrays.asList("select 1 /* c1; */", " /**/ select 2 /*/ c3; /*/", " select 3", " /* c4 */"),
          commands.getCmdList("select 1 /* c1; */; /**/ select 2 /*/ c3; /*/; select 3; /* c4 */", false)
      );
      // block comments with line comments
      assertEquals(
          Arrays.asList("select 1 --lc /* fake bc\n", "select 2 --lc */\n"),
          commands.getCmdList("select 1 --lc /* fake bc\n;select 2 --lc */\n;", false)
      );
      assertEquals(
          Arrays.asList("select 1 /*bc -- fake lc\n;select 2 --lc */\n"),
          commands.getCmdList("select 1 /*bc -- fake lc\n;select 2 --lc */\n;", false)
      );
      // block comments with quotation marks
      assertEquals(
          Arrays.asList("select 1 /* v'lid */", "select 2", "select 3"),
          commands.getCmdList("select 1 /* v'lid */;select 2;select 3;", false)
      );
      assertEquals(
          Arrays.asList("select 1 /* v\"lid */", "select 2", "select 3"),
          commands.getCmdList("select 1 /* v\"lid */;select 2;select 3;", false)
      );
      assertEquals(
          Arrays.asList("", "select 1 /* ' */", "select \"'\"", "select 3 /* \" */", "?"),
          commands.getCmdList(";select 1 /* ' */;select \"'\";select 3 /* \" */;?", false)
      );
      assertEquals(
          Arrays.asList("", "select 1 /*/ ' ;select \"'\" /*/", "select 3 /* \" */", "?"),
          commands.getCmdList(";select 1 /*/ ' ;select \"'\" /*/;select 3 /* \" */;?", false)
      );

      // UNTERMINATED STRING, COMMENT

      assertEquals(
          Arrays.asList("select 1", " -- ;\\';\\\";--;  ;/*;*/; '; ';\";\";"),
          commands.getCmdList("select 1; -- ;\\';\\\";--;  ;/*;*/; '; ';\";\";", false)
      );
      assertEquals(
          Arrays.asList("select 1", " /* ;\\';\\\";--;\n;/*;  ; '; ';\";\";"),
          commands.getCmdList("select 1; /* ;\\';\\\";--;\n;/*;  ; '; ';\";\";", false)
      );
      assertEquals(
          Arrays.asList("select 1", " '  ;\\';\\\";--;\n;/*;*/;  ;  ;\";\";"),
          commands.getCmdList("select 1; '  ;\\';\\\";--;\n;/*;*/;  ;  ;\";\";", false)
      );
      assertEquals(
          Arrays.asList("select 1", " \" ;\\';\\\";--;\n;/*;*/; '; ';  ;  ;"),
          commands.getCmdList("select 1; \" ;\\';\\\";--;\n;/*;*/; '; ';  ;  ;", false)
      );
    } finally {
      beeline.close();
    }
  }
}

