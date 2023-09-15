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

package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter.convertType;
import static org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter.emptyPlan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

class TestASTConverter {

  @Test
  void testConvertTypeWhenInputIsStruct() {
    List<RelDataTypeField> fields = asList(
        new RelDataTypeFieldImpl("a", 0, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER)),
        new RelDataTypeFieldImpl("b", 1, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.CHAR, 30)),
        new RelDataTypeFieldImpl("c", 2, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.NULL)));

    RelDataType dataType = new RelRecordType(fields);

    ASTNode tree = convertType(dataType);
    assertThat(tree.dump(), is(EXPECTED_STRUCT_TREE));
  }

  private static final String EXPECTED_STRUCT_TREE = "\n" +
      "TOK_STRUCT\n" +
      "   TOK_TABCOLLIST\n" +
      "      TOK_TABCOL\n" +
      "         a\n" +
      "         TOK_INT\n" +
      "      TOK_TABCOL\n" +
      "         b\n" +
      "         TOK_CHAR\n" +
      "            30\n" +
      "      TOK_TABCOL\n" +
      "         c\n" +
      "         TOK_NULL\n";

  @Test
  void testEmptyPlanWhenInputSchemaIsEmpty() {
    RelRecordType dataType = new RelRecordType(Collections.emptyList());
    IllegalArgumentException thrown = Assertions.assertThrows(IllegalArgumentException.class, () -> {
      emptyPlan(dataType);
    });

    Assertions.assertTrue(thrown.getMessage().contains("Schema is empty"));
  }

  @Test
  void testEmptyPlan() {
    List<RelDataTypeField> fields = asList(
        new RelDataTypeFieldImpl("a", 0, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER)),
        new RelDataTypeFieldImpl("b", 1, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.CHAR, 30)),
        new RelDataTypeFieldImpl("c", 2, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.NULL)));
    RelDataType dataType = new RelRecordType(fields);

    ASTNode tree = emptyPlan(dataType);

    assertThat(tree.dump(), is(EXPECTED_TREE));
  }

  private static final String EXPECTED_TREE = "\n" +
      "TOK_QUERY\n" +
      "   TOK_INSERT\n" +
      "      TOK_DESTINATION\n" +
      "         TOK_DIR\n" +
      "            TOK_TMP_FILE\n" +
      "      TOK_SELECT\n" +
      "         TOK_SELEXPR\n" +
      "            TOK_FUNCTION\n" +
      "               TOK_INT\n" +
      "               TOK_NULL\n" +
      "            a\n" +
      "         TOK_SELEXPR\n" +
      "            TOK_FUNCTION\n" +
      "               TOK_CHAR\n" +
      "                  30\n" +
      "               TOK_NULL\n" +
      "            b\n" +
      "         TOK_SELEXPR\n" +
      "            TOK_NULL\n" +
      "            c\n" +
      "      TOK_LIMIT\n" +
      "         0\n" +
      "         0\n";

  @Test
  void testEmptyPlanWithNestedComplexTypes() {
    List<RelDataTypeField> nestedStructFields = asList(
        new RelDataTypeFieldImpl("nf1", 0, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER)),
        new RelDataTypeFieldImpl("nf2", 1, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.CHAR, 30)));

    List<RelDataTypeField> structFields = asList(
        new RelDataTypeFieldImpl("f1", 0, new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER)),
        new RelDataTypeFieldImpl("farray", 1,
            new ArraySqlType(new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER), true)),
        new RelDataTypeFieldImpl("fmap", 2, new MapSqlType(
            new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER),
            new BasicSqlType(new HiveTypeSystemImpl(), SqlTypeName.INTEGER), true)),
        new RelDataTypeFieldImpl("fstruct", 3,
            new RelRecordType(nestedStructFields)));

    List<RelDataTypeField> fields = singletonList(new RelDataTypeFieldImpl("a", 0, new RelRecordType(structFields)));
    RelDataType dataType = new RelRecordType(fields);

    ASTNode tree = convertType(dataType);
    assertThat(tree.dump(), is(EXPECTED_COMPLEX_TREE));
  }

  private static final String EXPECTED_COMPLEX_TREE = "\n" +
      "TOK_STRUCT\n" +
      "   TOK_TABCOLLIST\n" +
      "      TOK_TABCOL\n" +
      "         a\n" +
      "         TOK_STRUCT\n" +
      "            TOK_TABCOLLIST\n" +
      "               TOK_TABCOL\n" +
      "                  f1\n" +
      "                  TOK_INT\n" +
      "               TOK_TABCOL\n" +
      "                  farray\n" +
      "                  TOK_LIST\n" +
      "                     TOK_INT\n" +
      "               TOK_TABCOL\n" +
      "                  fmap\n" +
      "                  TOK_MAP\n" +
      "                     TOK_INT\n" +
      "                     TOK_INT\n" +
      "               TOK_TABCOL\n" +
      "                  fstruct\n" +
      "                  TOK_STRUCT\n" +
      "                     TOK_TABCOLLIST\n" +
      "                        TOK_TABCOL\n" +
      "                           nf1\n" +
      "                           TOK_INT\n" +
      "                        TOK_TABCOL\n" +
      "                           nf2\n" +
      "                           TOK_CHAR\n" +
      "                              30\n";
}
