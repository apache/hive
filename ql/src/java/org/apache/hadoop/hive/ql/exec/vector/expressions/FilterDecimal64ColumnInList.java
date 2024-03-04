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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

/**
 * Evaluate IN filter on a batch for a vector of longs.
 */
public class FilterDecimal64ColumnInList extends FilterLongColumnInList {

  private static final long serialVersionUID = 1L;

  public FilterDecimal64ColumnInList() {
    super();
  }

  /**
   * After construction you must call setInListValues() to add the values to the IN set.
   */
  public FilterDecimal64ColumnInList(int colNum) {
    super(colNum);
  }

  @Override
  public Descriptor getDescriptor() {

    // This VectorExpression (IN) is a special case, so don't return a descriptor.
    return null;
  }

  @Override
  public String vectorExpressionParameters() {
    DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) inputTypeInfos[0];
    final int scale = decimalTypeInfo.scale();
    HiveDecimalWritable writable = new HiveDecimalWritable();
    StringBuilder sb = new StringBuilder();
    sb.append(getColumnParamString(0, inputCol));
    sb.append(", values [");
    for (long value : inListValues) {
      writable.deserialize64(value, scale);
      sb.append(", decimal64Val ");
      sb.append(value);
      sb.append(", decimalVal ");
      sb.append(writable.toString());
    }
    sb.append("]");
    return sb.toString();
  }
}
