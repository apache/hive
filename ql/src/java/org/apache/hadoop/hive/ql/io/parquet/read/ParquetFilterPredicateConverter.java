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
package org.apache.hadoop.hive.ql.io.parquet.read;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.io.parquet.FilterPredicateLeafBuilder;
import org.apache.hadoop.hive.ql.io.parquet.LeafFilterFactory;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetFilterPredicateConverter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetFilterPredicateConverter.class);

  /**
   * Translate the search argument to the filter predicate parquet uses. It includes
   * only the columns from the passed schema.
   * @return  a filter predicate translated from search argument. null is returned
   *          if failed to convert.
   */
  public static FilterPredicate toFilterPredicate(SearchArgument sarg, MessageType schema, Map<String, TypeInfo> columnTypes) {
    Map<String, TypeInfo> columns = null;
    if (schema != null) {
      columns = new HashMap<>();
      for (Type field : schema.getFields()) {
      String columnName = field.getName();
        columns.put(columnName, columnTypes.get(columnName));
      }
    }

    try {
      return translate(sarg.getExpression(), sarg.getLeaves(), columns, schema);
    } catch(Exception e) {
      return null;
    }
  }

  private static FilterPredicate translate(ExpressionTree root,
                                           List<PredicateLeaf> leaves,
                                           Map<String, TypeInfo> columns,
                                           MessageType schema) throws Exception {
    FilterPredicate p = null;
    switch (root.getOperator()) {
      case OR:
        for(ExpressionTree child: root.getChildren()) {
          FilterPredicate childPredicate = translate(child, leaves, columns, schema);
          if (childPredicate == null) {
            return null;
          }

          if (p == null) {
            p = childPredicate;
          } else {
            p = FilterApi.or(p, childPredicate);
          }
        }
        return p;
      case AND:
        for(ExpressionTree child: root.getChildren()) {
          if (p == null) {
            p = translate(child, leaves, columns, schema);
          } else {
            FilterPredicate right = translate(child, leaves, columns, schema);
            // constant means no filter, ignore it when it is null
            if(right != null){
              p = FilterApi.and(p, right);
            }
          }
        }
        return p;
      case NOT:
        FilterPredicate op = translate(root.getChildren().get(0), leaves,
            columns, schema);
        if (op != null) {
          return FilterApi.not(op);
        } else {
          return null;
        }
      case LEAF:
        PredicateLeaf leaf = leaves.get(root.getLeaf());

        // If columns is null, then we need to create the leaf
        if (columns.containsKey(leaf.getColumnName())) {
          Type parquetType = schema.getType(leaf.getColumnName());
          TypeInfo hiveType = columns.get(leaf.getColumnName());
          return buildFilterPredicateFromPredicateLeaf(leaf, parquetType, hiveType);
        } else {
          // Do not create predicate if the leaf is not on the passed schema.
          return null;
        }
      case CONSTANT:
        return null;// no filter will be executed for constant
      default:
        throw new IllegalStateException("Unknown operator: " +
            root.getOperator());
    }
  }

  private static FilterPredicate buildFilterPredicateFromPredicateLeaf
      (PredicateLeaf leaf, Type parquetType, TypeInfo columnType) throws Exception {
    LeafFilterFactory leafFilterFactory = new LeafFilterFactory();
    FilterPredicateLeafBuilder builder;
    try {
      builder = leafFilterFactory
          .getLeafFilterBuilderByType(leaf.getType(), parquetType);

      if (isMultiLiteralsOperator(leaf.getOperator())) {
        return builder.buildPredicate(leaf.getOperator(),
            leaf.getLiteralList(),
            leaf.getColumnName(),
            columnType);
      } else {
        return builder
            .buildPredict(leaf.getOperator(),
                leaf.getLiteral(),
                leaf.getColumnName(),
                columnType);
      }
    } catch (Exception e) {
      LOG.error("fail to build predicate filter leaf with errors" + e, e);
      throw e;
    }
  }

  private static boolean isMultiLiteralsOperator(PredicateLeaf.Operator op) {
    return (op == PredicateLeaf.Operator.IN) ||
        (op == PredicateLeaf.Operator.BETWEEN);
  }
}
