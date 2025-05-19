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

import org.junit.Assert;
import org.junit.Test;

public class TestParseCase {
  ParseDriver parseDriver = new ParseDriver();

  @Test
  public void testParseCaseWithOperandAndOneBranch() throws Exception {
    ASTNode tree = parseDriver.parseSelect("select case upper(col1) when 'A' then 'OK' else 'N/A' end from t1", null).getTree();

    String result = "\n" +
            "TOK_SELECT\n" +
            "   TOK_SELEXPR\n" +
            "      TOK_FUNCTION\n" +
            "         when\n" +
            "         =\n" +
            "            TOK_FUNCTION\n" +
            "               upper\n" +
            "               TOK_TABLE_OR_COL\n" +
            "                  col1\n" +
            "            'A'\n" +
            "         'OK'\n" +
            "         'N/A'\n";

    Assert.assertEquals(result, tree.dump());
  }

  @Test
  public void testParseCaseWithOperandAndMultipleBranches() throws Exception {
    ASTNode tree = parseDriver.parseSelect(
            "select case a" +
                    "  when 'B' then 'bean'" +
                    "  when 'A' then 'apple' else 'N/A' end from t1", null).getTree();

    String result = "\n" +
            "TOK_SELECT\n" +
            "   TOK_SELEXPR\n" +
            "      TOK_FUNCTION\n" +
            "         when\n" +
            "         =\n" +
            "            TOK_TABLE_OR_COL\n" +
            "               a\n" +
            "            'B'\n" +
            "         'bean'\n" +
            "         =\n" +
            "            TOK_TABLE_OR_COL\n" +
            "               a\n" +
            "            'A'\n" +
            "         'apple'\n" +
            "         'N/A'\n";

    Assert.assertEquals(result, tree.dump());
  }

  @Test
  public void testParseCaseWithOperandAndNoElse() throws Exception {
    ASTNode tree = parseDriver.parseSelect("select case a when 'A' then 'OK' end from t1", null).getTree();

    String result = "\n" +
            "TOK_SELECT\n" +
            "   TOK_SELEXPR\n" +
            "      TOK_FUNCTION\n" +
            "         when\n" +
            "         =\n" +
            "            TOK_TABLE_OR_COL\n" +
            "               a\n" +
            "            'A'\n" +
            "         'OK'\n";

    Assert.assertEquals(result, tree.dump());
  }
}
