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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHiveBetween {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  @Test
  public void testUnparseBetween() {
    assertEquals("col BETWEEN 10 AND 20", unparse(false, "col", 10L, 20L));
  }

  @Test
  public void testUnparseNotBetween() {
    assertEquals("field NOT BETWEEN 30 AND 40", unparse(true, "field", 30L, 40L));
  }

  private static String unparse(boolean negated, String name, long lower, long upper) {
    List<SqlNode> operands = List.of(
        SqlLiteral.createBoolean(negated, POS), // operand 0: negated flag
        new SqlIdentifier(name, POS), // operand 1: value
        SqlNumericLiteral.createExactNumeric(BigDecimal.valueOf(lower).toString(), POS), // operand 2: lower
        SqlNumericLiteral.createExactNumeric(BigDecimal.valueOf(upper).toString(), POS) // operand 3: upper
    );
    SqlBasicCall call = new SqlBasicCall(HiveBetween.INSTANCE, operands, POS);
    SqlPrettyWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config().withQuoteAllIdentifiers(false));
    HiveBetween.INSTANCE.unparse(writer, call, 0, 0);
    return writer.toString();
  }
}
