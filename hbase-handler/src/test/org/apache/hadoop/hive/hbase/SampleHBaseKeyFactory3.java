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

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.mapred.JobConf;

/**
 * Simple extension of {@link SampleHBaseKeyFactory2} with exception of using filters instead of start
 * and stop keys
 * */
public class SampleHBaseKeyFactory3 extends SampleHBaseKeyFactory2 {

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
      ExprNodeDesc predicate) {
    SampleHBasePredicateDecomposer decomposedPredicate = new SampleHBasePredicateDecomposer(keyMapping);
    return decomposedPredicate.decomposePredicate(keyMapping.columnName, predicate);
  }
}

class SampleHBasePredicateDecomposer extends AbstractHBaseKeyPredicateDecomposer {

  private static final int FIXED_LENGTH = 10;

  private ColumnMapping keyMapping;

  SampleHBasePredicateDecomposer(ColumnMapping keyMapping) {
    this.keyMapping = keyMapping;
  }

  @Override
  public HBaseScanRange getScanRange(List<IndexSearchCondition> searchConditions)
      throws Exception {
    Map<String, List<IndexSearchCondition>> fieldConds =
        new HashMap<String, List<IndexSearchCondition>>();
    for (IndexSearchCondition condition : searchConditions) {
      String fieldName = condition.getFields()[0];
      List<IndexSearchCondition> fieldCond = fieldConds.get(fieldName);
      if (fieldCond == null) {
        fieldConds.put(fieldName, fieldCond = new ArrayList<IndexSearchCondition>());
      }
      fieldCond.add(condition);
    }
    List<Filter> filters = new ArrayList<Filter>();
    HBaseScanRange range = new HBaseScanRange();

    StructTypeInfo type = (StructTypeInfo) keyMapping.columnType;
    for (String name : type.getAllStructFieldNames()) {
      List<IndexSearchCondition> fieldCond = fieldConds.get(name);
      if (fieldCond == null || fieldCond.size() > 2) {
        continue;
      }
      for (IndexSearchCondition condition : fieldCond) {
        if (condition.getConstantDesc().getValue() == null) {
          continue;
        }
        String comparisonOp = condition.getComparisonOp();
        String constantVal = String.valueOf(condition.getConstantDesc().getValue());

        byte[] valueAsBytes = toBinary(constantVal, FIXED_LENGTH, false, false);

        if (comparisonOp.endsWith("UDFOPEqualOrGreaterThan")) {
          filters.add(new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(valueAsBytes)));
        } else if (comparisonOp.endsWith("UDFOPGreaterThan")) {
          filters.add(new RowFilter(CompareOp.GREATER, new BinaryComparator(valueAsBytes)));
        } else if (comparisonOp.endsWith("UDFOPEqualOrLessThan")) {
          filters.add(new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(valueAsBytes)));
        } else if (comparisonOp.endsWith("UDFOPLessThan")) {
          filters.add(new RowFilter(CompareOp.LESS, new BinaryComparator(valueAsBytes)));
        } else {
          throw new IOException(comparisonOp + " is not a supported comparison operator");
        }
      }
    }
    if (!filters.isEmpty()) {
      range.addFilter(new FilterList(Operator.MUST_PASS_ALL, filters));
    }
    return range;
  }

  private byte[] toBinary(String value, int max, boolean end, boolean nextBA) {
    return toBinary(value.getBytes(), max, end, nextBA);
  }

  private byte[] toBinary(byte[] value, int max, boolean end, boolean nextBA) {
    byte[] bytes = new byte[max + 1];
    System.arraycopy(value, 0, bytes, 0, Math.min(value.length, max));
    if (end) {
      Arrays.fill(bytes, value.length, max, (byte) 0xff);
    }
    if (nextBA) {
      bytes[max] = 0x01;
    }
    return bytes;
  }
}