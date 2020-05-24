/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet;

import java.util.List;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;

import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

/**
 * The base class for building parquet supported filter predicate in primary types.
 */
public abstract class FilterPredicateLeafBuilder {

  /**
   * Build filter predicate with multiple constants
   *
   * @param op         IN or BETWEEN
   * @param literals
   * @param columnName
   * @param columnType
   * @return
   */
  public FilterPredicate buildPredicate(PredicateLeaf.Operator op, List<Object> literals,
                                        String columnName, TypeInfo columnType) throws Exception {
    FilterPredicate result = null;
    switch (op) {
      case IN:
        for (Object literal : literals) {
          if (result == null) {
            result = buildPredict(PredicateLeaf.Operator.EQUALS, literal, columnName, columnType);
          } else {
            result = or(result, buildPredict(PredicateLeaf.Operator.EQUALS, literal,
                columnName, columnType));
          }
        }
        return result;
      case BETWEEN:
        if (literals.size() != 2) {
          throw new RuntimeException(
            "Not able to build 'between' operation filter with " + literals +
              " which needs two literals");
        }
        Object min = literals.get(0);
        Object max = literals.get(1);
        FilterPredicate lt = not(buildPredict(PredicateLeaf.Operator.LESS_THAN,
            min, columnName, columnType));
        FilterPredicate gt = buildPredict(PredicateLeaf.Operator.LESS_THAN_EQUALS, max, columnName, columnType);
        result = FilterApi.and(gt, lt);
        return result;
      default:
        throw new RuntimeException("Unknown PredicateLeaf Operator type: " + op);
    }
  }

  /**
   * Build predicate with a single constant
   *
   * @param op         EQUALS, NULL_SAFE_EQUALS, LESS_THAN, LESS_THAN_EQUALS, IS_NULL
   * @param constant
   * @param columnName
   * @param columnType
   * @return null or a FilterPredicate, null means no filter will be executed
   */
  public abstract FilterPredicate buildPredict(PredicateLeaf.Operator op, Object constant,
                                               String columnName, TypeInfo columnType) throws Exception;
}
