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
package org.apache.hadoop.hive.ql.io.parquet.write;

import org.apache.hadoop.hive.ql.io.parquet.writable.BigDecimalWritable;
import org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import parquet.io.ParquetEncodingException;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.Type;

/**
 *
 * DataWritableWriter is a writer,
 * that will read an ArrayWritable and give the data to parquet
 * with the expected schema
 *
 */
public class DataWritableWriter {

  private final RecordConsumer recordConsumer;
  private final GroupType schema;

  public DataWritableWriter(final RecordConsumer recordConsumer, final GroupType schema) {
    this.recordConsumer = recordConsumer;
    this.schema = schema;
  }

  public void write(final ArrayWritable arr) {
    if (arr == null) {
      return;
    }
    recordConsumer.startMessage();
    writeData(arr, schema);
    recordConsumer.endMessage();
  }

  private void writeData(final ArrayWritable arr, final GroupType type) {
    if (arr == null) {
      return;
    }
    final int fieldCount = type.getFieldCount();
    Writable[] values = arr.get();
    for (int field = 0; field < fieldCount; ++field) {
      final Type fieldType = type.getType(field);
      final String fieldName = fieldType.getName();
      final Writable value = values[field];
      if (value == null) {
        continue;
      }
      recordConsumer.startField(fieldName, field);

      if (fieldType.isPrimitive()) {
        writePrimitive(value);
      } else {
        recordConsumer.startGroup();
        if (value instanceof ArrayWritable) {
          if (fieldType.asGroupType().getRepetition().equals(Type.Repetition.REPEATED)) {
            writeArray((ArrayWritable) value, fieldType.asGroupType());
          } else {
            writeData((ArrayWritable) value, fieldType.asGroupType());
          }
        } else if (value != null) {
          throw new ParquetEncodingException("This should be an ArrayWritable or MapWritable: " + value);
        }

        recordConsumer.endGroup();
      }

      recordConsumer.endField(fieldName, field);
    }
  }

  private void writeArray(final ArrayWritable array, final GroupType type) {
    if (array == null) {
      return;
    }
    final Writable[] subValues = array.get();
    final int fieldCount = type.getFieldCount();
    for (int field = 0; field < fieldCount; ++field) {
      final Type subType = type.getType(field);
      recordConsumer.startField(subType.getName(), field);
      for (int i = 0; i < subValues.length; ++i) {
        final Writable subValue = subValues[i];
        if (subValue != null) {
          if (subType.isPrimitive()) {
            if (subValue instanceof ArrayWritable) {
              writePrimitive(((ArrayWritable) subValue).get()[field]);// 0 ?
            } else {
              writePrimitive(subValue);
            }
          } else {
            if (!(subValue instanceof ArrayWritable)) {
              throw new RuntimeException("This should be a ArrayWritable: " + subValue);
            } else {
              recordConsumer.startGroup();
              writeData((ArrayWritable) subValue, subType.asGroupType());
              recordConsumer.endGroup();
            }
          }
        }
      }
      recordConsumer.endField(subType.getName(), field);
    }
  }

  private void writePrimitive(final Writable value) {
    if (value == null) {
      return;
    }
    if (value instanceof DoubleWritable) {
      recordConsumer.addDouble(((DoubleWritable) value).get());
    } else if (value instanceof BooleanWritable) {
      recordConsumer.addBoolean(((BooleanWritable) value).get());
    } else if (value instanceof FloatWritable) {
      recordConsumer.addFloat(((FloatWritable) value).get());
    } else if (value instanceof IntWritable) {
      recordConsumer.addInteger(((IntWritable) value).get());
    } else if (value instanceof LongWritable) {
      recordConsumer.addLong(((LongWritable) value).get());
    } else if (value instanceof ShortWritable) {
      recordConsumer.addInteger(((ShortWritable) value).get());
    } else if (value instanceof ByteWritable) {
      recordConsumer.addInteger(((ByteWritable) value).get());
    } else if (value instanceof BigDecimalWritable) {
      throw new UnsupportedOperationException("BigDecimal writing not implemented");
    } else if (value instanceof BinaryWritable) {
      recordConsumer.addBinary(((BinaryWritable) value).getBinary());
    } else {
      throw new IllegalArgumentException("Unknown value type: " + value + " " + value.getClass());
    }
  }
}
