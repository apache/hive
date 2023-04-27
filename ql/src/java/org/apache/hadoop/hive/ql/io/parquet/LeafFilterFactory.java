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

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
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

  @VisibleForTesting
  public static final String FILTER_PREDICATE_CONVERSION_NOT_SUPPORTED =
      "The conversion to Parquet FilterPredicate is not supported for %s. Please try to set the following configurations at the session level\n set hive.optimize.index.filter=false;\n"
          + " set hive.optimize.ppd=false;\n";

  class IntFilterPredicateLeafBuilder extends FilterPredicateLeafBuilder {
    /**
     * @param op         consists of EQUALS, NULL_SAFE_EQUALS, LESS_THAN, LESS_THAN_EQUALS, IS_NULL
     * @param literal
     * @param columnName
     * @param columnType
     * @return
     */
    @Override
    public FilterPredicate buildPredict(Operator op, Object literal,
                                        String columnName, TypeInfo columnType) {
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
                                        String columnName, TypeInfo columnType) {
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
    public FilterPredicate buildPredict(Operator op, Object constant, String columnName, TypeInfo columnType) {
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
                                        String columnName, TypeInfo columnType) {
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
                                        String columnName, TypeInfo columnType) throws Exception{
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
                                        String columnName, TypeInfo columnType) throws Exception{
      // For CHAR types, the trailing spaces should be removed before adding the
      // value to the predicate. This change is needed because for CHAR types,
      // Hive passes a padded value to the predicate, but since the value
      // is stored in Parquet without padding, no result would be returned.
      // For more details about this issue, please refer to HIVE-21407.
      String value = null;
      if (constant != null) {
        value = (String) constant;
        if (columnType != null && columnType.toString().startsWith(serdeConstants.CHAR_TYPE_NAME)) {
          value = removeTrailingSpaces(value);
        }
      }
      switch (op) {
        case LESS_THAN:
          return lt(binaryColumn(columnName), Binary.fromString(value));
        case IS_NULL:
        case EQUALS:
        case NULL_SAFE_EQUALS:
          return eq(binaryColumn(columnName),
            (constant == null) ? null : Binary.fromString(value));
        case LESS_THAN_EQUALS:
          return ltEq(binaryColumn(columnName), Binary.fromString(value));
        default:
          // should never be executed
          throw new RuntimeException("Unknown PredicateLeaf Operator type: " + op);
      }
    }

    private String removeTrailingSpaces(String value) {
      if (value == null) {
        return null;
      }
      String regex = "\\s+$";
      return value.replaceAll(regex, "");
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
      case FLOAT:
      case STRING:  // string, char, varchar
      case BOOLEAN:
        return getLeafFilterBuilderByParquetType(parquetType);
      case DATE:
      case DECIMAL:
      case TIMESTAMP:
      default:
        String msg = String.format(FILTER_PREDICATE_CONVERSION_NOT_SUPPORTED, type);
        LOG.debug(msg);
        throw new HiveException(msg);
    }
  }

  /**
   * Creates FilterPredicateLeafBuilder as per Parquet FileSchema type
   * @param parquetType
   * @return
   * @throws HiveException
   */
  private FilterPredicateLeafBuilder getLeafFilterBuilderByParquetType(Type parquetType) throws HiveException {
    switch (parquetType.asPrimitiveType().getPrimitiveTypeName()){
      case INT32: // TINYINT, SMALLINT, INT
        return new IntFilterPredicateLeafBuilder();
      case INT64: // LONG
        return new LongFilterPredicateLeafBuilder();
      case FLOAT:
        return new FloatFilterPredicateLeafBuilder();
      case DOUBLE:
        return new DoubleFilterPredicateLeafBuilder();
      case BINARY: // STRING, CHAR, VARCHAR
        return new BinaryFilterPredicateLeafBuilder();
      case BOOLEAN:
        return new BooleanFilterPredicateLeafBuilder();
      default:
        String msg = String.format(FILTER_PREDICATE_CONVERSION_NOT_SUPPORTED, parquetType.asPrimitiveType().getPrimitiveTypeName());
        LOG.debug(msg);
        throw new HiveException(msg);
    }
  }
}
