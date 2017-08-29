/**
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;

public class LeafFilterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(LeafFilterFactory.class);

  class IntFilterPredicateLeafBuilder extends FilterPredicateLeafBuilder {
    /**
     * @param op         consists of EQUALS, NULL_SAFE_EQUALS, LESS_THAN, LESS_THAN_EQUALS, IS_NULL
     * @param literal
     * @param columnName
     * @return
     */
    @Override
    public FilterPredicate buildPredict(Operator op, Object literal,
                                        String columnName) {
      switch (op) {
        case LESS_THAN:
          return lt(intColumn(columnName), ((Number) literal).intValue());
        case IS_NULL:
        case EQUALS:
        case NULL_SAFE_EQUALS:
          return eq(intColumn(columnName),
            (literal == null) ? null : ((Number) literal).intValue());
        case LESS_THAN_EQUALS:
          return ltEq(intColumn(columnName), ((Number) literal).intValue());
        default:
          throw new RuntimeException("Unknown PredicateLeaf Operator type: " + op);
      }
    }
  }

  class LongFilterPredicateLeafBuilder extends FilterPredicateLeafBuilder {
    @Override
    public FilterPredicate buildPredict(Operator op, Object constant,
                                        String columnName) {
      switch (op) {
        case LESS_THAN:
          return lt(FilterApi.longColumn(columnName), ((Number) constant).longValue());
        case IS_NULL:
        case EQUALS:
        case NULL_SAFE_EQUALS:
          return eq(FilterApi.longColumn(columnName),
            (constant == null) ? null : ((Number) constant).longValue());
        case LESS_THAN_EQUALS:
          return ltEq(FilterApi.longColumn(columnName),
            ((Number) constant).longValue());
        default:
          throw new RuntimeException("Unknown PredicateLeaf Operator type: " + op);
      }
    }
  }

  class FloatFilterPredicateLeafBuilder extends FilterPredicateLeafBuilder {
    @Override
    public FilterPredicate buildPredict(Operator op, Object constant, String columnName) {
      switch (op) {
      case LESS_THAN:
        return lt(floatColumn(columnName), ((Number) constant).floatValue());
      case IS_NULL:
      case EQUALS:
      case NULL_SAFE_EQUALS:
        return eq(floatColumn(columnName),
            (constant == null) ? null : ((Number) constant).floatValue());
      case LESS_THAN_EQUALS:
        return ltEq(FilterApi.floatColumn(columnName), ((Number) constant).floatValue());
      default:
        throw new RuntimeException("Unknown PredicateLeaf Operator type: " + op);
      }
    }
  }

  class DoubleFilterPredicateLeafBuilder extends FilterPredicateLeafBuilder {

    @Override
    public FilterPredicate buildPredict(Operator op, Object constant,
                                        String columnName) {
      switch (op) {
        case LESS_THAN:
          return lt(doubleColumn(columnName), ((Number) constant).doubleValue());
        case IS_NULL:
        case EQUALS:
        case NULL_SAFE_EQUALS:
          return eq(doubleColumn(columnName),
            (constant == null) ? null : ((Number) constant).doubleValue());
        case LESS_THAN_EQUALS:
          return ltEq(FilterApi.doubleColumn(columnName),
            ((Number) constant).doubleValue());
        default:
          throw new RuntimeException("Unknown PredicateLeaf Operator type: " + op);
      }
    }
  }

  class BooleanFilterPredicateLeafBuilder extends FilterPredicateLeafBuilder {
    @Override
    public FilterPredicate buildPredict(Operator op, Object constant,
                                        String columnName) throws Exception{
      switch (op) {
        case IS_NULL:
        case EQUALS:
        case NULL_SAFE_EQUALS:
          return eq(booleanColumn(columnName),
            (constant == null) ? null : ((Boolean) constant).booleanValue());
        default:
          throw new RuntimeException("Unknown PredicateLeaf Operator type: " + op);
      }
    }
  }

  class BinaryFilterPredicateLeafBuilder extends FilterPredicateLeafBuilder {
    @Override
    public FilterPredicate buildPredict(Operator op, Object constant,
                                        String columnName) throws Exception{
      switch (op) {
        case LESS_THAN:
          return lt(binaryColumn(columnName), Binary.fromString((String) constant));
        case IS_NULL:
        case EQUALS:
        case NULL_SAFE_EQUALS:
          return eq(binaryColumn(columnName),
            (constant == null) ? null : Binary.fromString((String) constant));
        case LESS_THAN_EQUALS:
          return ltEq(binaryColumn(columnName), Binary.fromString((String) constant));
        default:
          // should never be executed
          throw new RuntimeException("Unknown PredicateLeaf Operator type: " + op);
      }
    }
  }

  /**
   * get leaf filter builder by FilterPredicateType, currently date, decimal and timestamp is not
   * supported yet.
   * @param type FilterPredicateType
   * @return
   * @throws HiveException Exception is thrown for unsupported data types so we can skip filtering
   */
  public FilterPredicateLeafBuilder getLeafFilterBuilderByType(
      PredicateLeaf.Type type,
      Type parquetType) throws HiveException {
    switch (type){
      case LONG:
        if (parquetType.asPrimitiveType().getPrimitiveTypeName() ==
            PrimitiveType.PrimitiveTypeName.INT32) {
          return new IntFilterPredicateLeafBuilder();
        } else {
          return new LongFilterPredicateLeafBuilder();
        }
      case FLOAT:
        if (parquetType.asPrimitiveType().getPrimitiveTypeName() ==
            PrimitiveType.PrimitiveTypeName.FLOAT) {
          return new FloatFilterPredicateLeafBuilder();
        } else {
          return new DoubleFilterPredicateLeafBuilder();
        }
      case STRING:  // string, char, varchar
        return new BinaryFilterPredicateLeafBuilder();
      case BOOLEAN:
        return new BooleanFilterPredicateLeafBuilder();
      case DATE:
      case DECIMAL:
      case TIMESTAMP:
      default:
        String msg = "Conversion to Parquet FilterPredicate not supported for " + type;
        LOG.debug(msg);
        throw new HiveException(msg);
    }
  }
}
