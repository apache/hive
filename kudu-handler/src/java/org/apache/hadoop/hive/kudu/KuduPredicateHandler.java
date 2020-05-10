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
package org.apache.hadoop.hive.kudu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Contains static methods for decomposing predicate/filter expressions and
 * getting the equivalent Kudu predicates.
 */
public final class KuduPredicateHandler {
  static final Logger LOG = LoggerFactory.getLogger(KuduPredicateHandler.class);

  private KuduPredicateHandler() {}

  /**
   * Analyzes the predicates and return the portion of it which
   * cannot be evaluated by Kudu during table access.
   *
   * @param predicateExpr predicate to be decomposed
   * @param schema the schema of the Kudu table
   * @return decomposed form of predicate, or null if no pushdown is possible at all
   */
  public static DecomposedPredicate decompose(ExprNodeDesc predicateExpr, Schema schema) {
    IndexPredicateAnalyzer analyzer = newAnalyzer(schema);
    List<IndexSearchCondition> sConditions = new ArrayList<>();
    ExprNodeDesc residualPredicate = analyzer.analyzePredicate(predicateExpr, sConditions);

    // Nothing to decompose.
    if (sConditions.size() == 0) {
      return null;
    }

    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(sConditions);
    decomposedPredicate.residualPredicate = (ExprNodeGenericFuncDesc) residualPredicate;
    return decomposedPredicate;
  }

  private static IndexPredicateAnalyzer newAnalyzer(Schema schema) {
    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

    // Register comparison operators which can be satisfied by Kudu predicates.
    // Note: We are unable to decompose GenericUDFOPNull, GenericUDFOPNotNull, GenericUDFIn
    // predicates even though they are pushed to Kudu because the IndexPredicateAnalyzer only
    // supports GenericUDFBaseCompare UDFs.
    // We can also handle some NOT predicates but the IndexPredicateAnalyzer also does
    // not support this.
    analyzer.addComparisonOp(GenericUDFOPEqual.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualNS.class.getName());
    analyzer.addComparisonOp(GenericUDFOPGreaterThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrGreaterThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPLessThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrLessThan.class.getName());

    // Set the column names that can be satisfied.
    for (ColumnSchema col : schema.getColumns()) {
      // Skip binary columns because binary predicates are not supported. (HIVE-11370)
      if (col.getType() != Type.BINARY) {
        analyzer.allowColumnName(col.getName());
      }
    }

    return analyzer;
  }

  /**
   * Returns the list of Kudu predicates from the passed configuration.
   *
   * @param conf the execution configuration
   * @param schema the schema of the Kudu table
   * @return the list of Kudu predicates
   */
  public static List<KuduPredicate> getPredicates(Configuration conf, Schema schema) {
    SearchArgument sarg = ConvertAstToSearchArg.createFromConf(conf);
    if (sarg == null) {
      return Collections.emptyList();
    }
    return toKuduPredicates(sarg, schema);
  }

  /**
   * Translate the search argument to the KuduPredicates.
   */
  private static List<KuduPredicate> toKuduPredicates(SearchArgument sarg, Schema schema) {
    List<KuduPredicate> results = new ArrayList<>();
    try {
      translate(sarg.getExpression(), sarg.getLeaves(), false, schema, results);
    } catch(Exception ex) {
      LOG.warn("Exception while generating Kudu predicates. Predicates will not be pushed", ex);
      return Collections.emptyList();
    }
    return results;
  }

  private static void translate(ExpressionTree root, List<PredicateLeaf> leaves, boolean isNot,
                                Schema schema, List<KuduPredicate> results) {
    switch (root.getOperator()) {
    case OR:
      if (isNot) {
        // Converts to an AND: not (A or B) = not A and not B
        for(ExpressionTree child: root.getChildren()) {
          translate(child, leaves, isNot, schema, results);
        }
      } else {
        // Kudu doesn't support OR predicates.
        return;
      }
    case AND:
      if (isNot) {
        // Converts to an OR: not (A and B) = not A or not B
        // Kudu doesn't support OR predicates.
        return;
      } else {
        for(ExpressionTree child: root.getChildren()) {
          translate(child, leaves, isNot, schema, results);
        }
        return;
      }
    case NOT:
      // Kudu doesn't support general NOT predicates, but some NOT operators
      // can be converted into Kudu predicates. See leafToPredicates below.
      translate(root.getChildren().get(0), leaves, !isNot, schema, results);
      return;
    case LEAF:
      PredicateLeaf leaf = leaves.get(root.getLeaf());
      if (schema.hasColumn(leaf.getColumnName())) {
        results.addAll(leafToPredicates(leaf, isNot, schema));
      }
      return;
    case CONSTANT:
      return; // no predicate for constant
    default:
      throw new IllegalStateException("Unknown operator: " + root.getOperator());
    }
  }

  private static List<KuduPredicate> leafToPredicates(PredicateLeaf leaf, boolean isNot, Schema schema) {
    ColumnSchema column = schema.getColumn(leaf.getColumnName());
    Object value = leaf.getLiteral();

    switch (leaf.getOperator()) {
    case EQUALS:
      if (isNot) {
        // Kudu doesn't support NOT EQUALS.
        return Collections.emptyList();
      } else {
        return Collections.singletonList(KuduPredicate.newComparisonPredicate(column,
            ComparisonOp.EQUAL, toKuduValue(value, column)));
      }
    case LESS_THAN:
      if (isNot) {
        return Collections.singletonList(KuduPredicate.newComparisonPredicate(column,
            ComparisonOp.GREATER_EQUAL, toKuduValue(value, column)));
      } else {
        return Collections.singletonList(KuduPredicate.newComparisonPredicate(column,
            ComparisonOp.LESS, toKuduValue(value, column)));
      }
    case LESS_THAN_EQUALS:
      if (isNot) {
        return Collections.singletonList(KuduPredicate.newComparisonPredicate(column,
            ComparisonOp.GREATER, toKuduValue(value, column)));
      } else {
        return Collections.singletonList(KuduPredicate.newComparisonPredicate(column,
            ComparisonOp.LESS_EQUAL, toKuduValue(value, column)));
      }
    case IS_NULL:
      if (isNot) {
        return Collections.singletonList(KuduPredicate.newIsNotNullPredicate(column));
      } else {
        return Collections.singletonList(KuduPredicate.newIsNullPredicate(column));
      }
    case IN:
      if (isNot) {
        // Kudu doesn't support NOT IN.
        return Collections.emptyList();
      } else {
        List<Object> values = leaf.getLiteralList().stream()
            .map((Object v) -> toKuduValue(v, column))
            .collect(Collectors.toList());
        return Collections.singletonList(KuduPredicate.newInListPredicate(column, values));
      }
    case BETWEEN:
      List<Object> values = leaf.getLiteralList();
      Object leftValue = toKuduValue(values.get(0), column);
      Object rightValue = toKuduValue(values.get(1), column);
      if (isNot) {
        return Arrays.asList(
            KuduPredicate.newComparisonPredicate(column, ComparisonOp.LESS, leftValue),
            KuduPredicate.newComparisonPredicate(column, ComparisonOp.GREATER, rightValue)
        );
      } else {
        return Arrays.asList(
            KuduPredicate.newComparisonPredicate(column, ComparisonOp.GREATER_EQUAL, leftValue),
            KuduPredicate.newComparisonPredicate(column, ComparisonOp.LESS_EQUAL, rightValue)
        );
      }
    case NULL_SAFE_EQUALS:
      // Kudu doesn't support null value predicates.
      return Collections.emptyList();
    default:
      throw new RuntimeException("Unhandled operator: " + leaf.getOperator());
    }
  }

  // Converts Hive value objects to the value Objects expected by Kudu.
  private static Object toKuduValue(Object value, ColumnSchema column) {
    if (value instanceof HiveDecimalWritable) {
      return ((HiveDecimalWritable) value).getHiveDecimal().bigDecimalValue();
    } else if (value instanceof Timestamp) {
      // Calling toSqlTimestamp and using the addTimestamp API ensures we properly
      // convert Hive localDateTime to UTC.
      return ((Timestamp) value).toSqlTimestamp();
    } else if (value instanceof Double && column.getType() == Type.FLOAT) {
      // Hive search arguments use Double for both FLOAT and DOUBLE columns.
      // Down convert to match the FLOAT columns type.
      return ((Double) value).floatValue();
    } else {
      return value;
    }
  }
}
