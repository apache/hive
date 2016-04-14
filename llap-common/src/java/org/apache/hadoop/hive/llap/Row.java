/**
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
package org.apache.hadoop.hive.llap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class Row {
  private final Schema schema;
  private final Writable[] colValues;
  private final boolean[] nullIndicators;
  private Map<String, Integer> nameToIndexMapping;

  public Row(Schema schema) {
    this.schema = schema;
    this.colValues = new Writable[schema.getColumns().size()];
    this.nullIndicators = new boolean[schema.getColumns().size()];
    this.nameToIndexMapping = new HashMap<String, Integer>(schema.getColumns().size());

    List<FieldDesc> colDescs = schema.getColumns();
    for (int idx = 0; idx < colDescs.size(); ++idx) {
      FieldDesc colDesc = colDescs.get(idx);
      nameToIndexMapping.put(colDesc.getName(), idx);
      colValues[idx] = createWritableForType(colDesc.getTypeDesc());
    }
  }

  public Writable getValue(int colIndex) {
    if (nullIndicators[colIndex]) {
      return null;
    }
    return colValues[colIndex];
  }

  public Writable getValue(String colName) {
    Integer idx = nameToIndexMapping.get(colName);
    Preconditions.checkArgument(idx != null);
    return getValue(idx);
  }

  public Schema getSchema() {
    return schema;
  }

  void setValue(int colIdx, Writable value) {
    Preconditions.checkArgument(colIdx <= schema.getColumns().size());

    if (value == null) {
      nullIndicators[colIdx] = true;
    } else {
      nullIndicators[colIdx] = false;
      FieldDesc colDesc = schema.getColumns().get(colIdx);
      switch (colDesc.getTypeDesc().getType()) {
        case BOOLEAN:
          ((BooleanWritable) colValues[colIdx]).set(((BooleanWritable) value).get());
          break;
        case TINYINT:
          ((ByteWritable) colValues[colIdx]).set(((ByteWritable) value).get());
          break;
        case SMALLINT:
          ((ShortWritable) colValues[colIdx]).set(((ShortWritable) value).get());
          break;
        case INT:
          ((IntWritable) colValues[colIdx]).set(((IntWritable) value).get());
          break;
        case BIGINT:
          ((LongWritable) colValues[colIdx]).set(((LongWritable) value).get());
          break;
        case FLOAT:
          ((FloatWritable) colValues[colIdx]).set(((FloatWritable) value).get());
          break;
        case DOUBLE:
          ((DoubleWritable) colValues[colIdx]).set(((DoubleWritable) value).get());
          break;
        case STRING:
        // Just handle char/varchar as Text
        case CHAR:
        case VARCHAR:
          ((Text) colValues[colIdx]).set((Text) value);
          break;
        case DATE:
          ((DateWritable) colValues[colIdx]).set((DateWritable) value);
          break;
        case TIMESTAMP:
          ((TimestampWritable) colValues[colIdx]).set((TimestampWritable) value);
          break;
        case BINARY:
          ((BytesWritable) colValues[colIdx]).set(((BytesWritable) value));
          break;
        case DECIMAL:
          ((HiveDecimalWritable) colValues[colIdx]).set((HiveDecimalWritable) value);
          break;
      }
    }
  }

  private Writable createWritableForType(TypeDesc typeDesc) {
    switch (typeDesc.getType()) {
      case BOOLEAN:
        return new BooleanWritable();
      case TINYINT:
        return new ByteWritable();
      case SMALLINT:
        return new ShortWritable();
      case INT:
        return new IntWritable();
      case BIGINT:
        return new LongWritable();
      case FLOAT:
        return new FloatWritable();
      case DOUBLE:
        return new DoubleWritable();
      case STRING:
      // Just handle char/varchar as Text
      case CHAR:
      case VARCHAR:
        return new Text();
      case DATE:
        return new DateWritable();
      case TIMESTAMP:
        return new TimestampWritable();
      case BINARY:
        return new BytesWritable();
      case DECIMAL:
        return new HiveDecimalWritable();
      default:
        throw new RuntimeException("Cannot create writable for " + typeDesc.getType());
    }
  }
}
