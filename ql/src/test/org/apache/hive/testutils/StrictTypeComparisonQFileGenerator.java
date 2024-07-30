/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.testutils;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * A generator for creating end-to-end tests (.q files) for covering various combinations of strict type comparisons.
 * The generator allows to have a more principled way to write tests without missing some cases. Having a programmatic
 * way for generating the queries makes it easier to add/remove combinations.The generator aids in the query generation
 * but the results (.q.out) still need to be verified manually.
 * <p>Currently, the generator is used to create the content for {@code strict_type_comparisons.q} file.</p>
 */
public class StrictTypeComparisonQFileGenerator {

  private static final char NL = '\n';
  private static final String TABLE_NAME = "t1";
  private static final String ID_COLUMN = "id";
  private static final int ROW_NUM = 4;
  private static final Map<String, String> COLUMNS = ImmutableMap.<String, String>builder()
      .put("c_bigint", "BIGINT")
      .put("c_string", "STRING")
      .put("c_decimal_19_0", "DECIMAL(19,0)")
      .put("c_double", "DOUBLE")
      .build();

  public static void generateAll(Consumer<String> sink) {
    sink.accept(comment("File generated using " + StrictTypeComparisonQFileGenerator.class.getSimpleName()));
    sink.accept(comment("Don't modify directly. Add new test cases by changing the generator"));
    sink.accept(generateDDL());
    sink.accept(generateDML());
    sink.accept("set hive.strict.checks.type.safety=false;" + NL);
    generateSQL(sink);
  }

  private static String comment(String message) {
    return "-- " + message + NL;
  }

  private static String generateDDL() {
    final String allColumns =
        COLUMNS.entrySet().stream().map(c -> c.getKey() + " " + c.getValue()).collect(Collectors.joining(","));
    return "CREATE TABLE " + TABLE_NAME + " (" + ID_COLUMN + " CHAR(1)," + allColumns + ");" + NL;
  }

  private static String generateDML() {
    PrimitiveIterator.OfLong decrementalIterator = LongStream.iterate(Long.MAX_VALUE - 100, l -> l - 1).iterator();
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO TABLE ");
    sb.append(TABLE_NAME);
    sb.append(" VALUES ");
    sb.append(NL);
    char cid = 'A';
    for (int i = 0; i < ROW_NUM; i++) {
      if (i > 0) {
        sb.append(',');
        sb.append(NL);
      }
      sb.append("('");
      sb.append(cid++);
      sb.append("',");
      final LongSupplier columnValGenerator;
      if (i < (ROW_NUM / 2)) {
        // Use the same value in every column
        long staticGenerator = Long.MAX_VALUE - i;
        columnValGenerator = () -> staticGenerator;
      } else {
        // Use different value in every column
        columnValGenerator = decrementalIterator::nextLong;
      }
      sb.append(COLUMNS.values().stream().map(type -> longToTypeLiteral(columnValGenerator.getAsLong(), type))
          .collect(Collectors.joining(",")));
      sb.append(')');
    }
    sb.append(';');
    sb.append(NL);
    return sb.toString();
  }

  private static String longToTypeLiteral(long value, String type) {
    switch (type) {
    case "STRING":
      return "'" + value + "'";
    case "DOUBLE":
      return value + "D";
    case "BIGINT":
    case "DECIMAL(19,0)":
    default:
      return String.valueOf(value);
    }
  }

  private static void generateSQL(Consumer<String> sink) {
    List<String> constants = new ArrayList<>();
    constants.add("9223372036854775807");
    constants.add("'9223372036854775807'");
    constants.add("9223372036854775807L");
    constants.add("9223372036854775807BD");
    constants.add("9223372036854775807D");
    constants.add("922337203685477580.7E+1");
    constants.add("92233720368547758070E-1");
    sink.accept(comment("Comparisons between a COLUMN and a CONSTANT"));
    for (String col : COLUMNS.keySet()) {
      for (String con : constants) {
        for (ExplainType explain : ExplainType.values()) {
          sink.accept(generateSQL(explain, col, con));
        }
      }
    }
    sink.accept(comment("Comparisons between two COLUMNS of different types"));
    for (String c1 : COLUMNS.keySet()) {
      for (String c2 : COLUMNS.keySet()) {
        if (c1.equals(c2)) {
          continue;
        }
        for (ExplainType explain : ExplainType.values()) {
          sink.accept(generateSQL(explain, c1, c2));
        }
      }
    }
  }

  private enum ExplainType {
    NO, EXPLAIN
  }

  private static String generateSQL(ExplainType type, String leftColumn, String rightColumn) {
    final String q = "SELECT " + ID_COLUMN + " FROM t1 WHERE " + leftColumn + " = " + rightColumn + ";" + NL;
    switch (type) {
    case NO:
      return q;
    case EXPLAIN:
      return "EXPLAIN " + q;
    default:
      throw new IllegalStateException();
    }
  }
}
