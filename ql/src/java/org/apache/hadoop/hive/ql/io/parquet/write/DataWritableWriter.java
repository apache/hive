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

import java.sql.Timestamp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.OriginalType;
import parquet.schema.Type;

/**
 *
 * DataWritableWriter is a writer,
 * that will read an ArrayWritable and give the data to parquet
 * with the expected schema
 * This is a helper class used by DataWritableWriteSupport class.
 */
public class DataWritableWriter {
  private static final Log LOG = LogFactory.getLog(DataWritableWriter.class);
  private final RecordConsumer recordConsumer;
  private final GroupType schema;

  public DataWritableWriter(final RecordConsumer recordConsumer, final GroupType schema) {
    this.recordConsumer = recordConsumer;
    this.schema = schema;
  }

  /**
   * It writes all record values to the Parquet RecordConsumer.
   * @param record Contains the record of values that are going to be written
   */
  public void write(final ArrayWritable record) {
    if (record != null) {
      recordConsumer.startMessage();
      try {
        writeGroupFields(record, schema);
      } catch (RuntimeException e) {
        String errorMessage = "Parquet record is malformed: " + e.getMessage();
        LOG.error(errorMessage, e);
        throw new RuntimeException(errorMessage, e);
      }
      recordConsumer.endMessage();
    }
  }

  /**
   * It writes all the fields contained inside a group to the RecordConsumer.
   * @param value The list of values contained in the group.
   * @param type Type that contains information about the group schema.
   */
  public void writeGroupFields(final ArrayWritable value, final GroupType type) {
    if (value != null) {
      for (int i = 0; i < type.getFieldCount(); i++) {
        Type fieldType = type.getType(i);
        String fieldName = fieldType.getName();
        Writable fieldValue = value.get()[i];

        // Parquet does not write null elements
        if (fieldValue != null) {
          recordConsumer.startField(fieldName, i);
          writeValue(fieldValue, fieldType);
          recordConsumer.endField(fieldName, i);
        }
      }
    }
  }

  /**
   * It writes the field value to the Parquet RecordConsumer. It detects the field type, and writes
   * the correct write function.
   * @param value The writable object that contains the value.
   * @param type Type that contains information about the type schema.
   */
  private void writeValue(final Writable value, final Type type) {
    if (type.isPrimitive()) {
      writePrimitive(value);
    } else if (value instanceof ArrayWritable) {
      GroupType groupType = type.asGroupType();
      OriginalType originalType = type.getOriginalType();

      if (originalType != null && originalType.equals(OriginalType.LIST)) {
        writeArray((ArrayWritable)value, groupType);
      } else if (originalType != null && originalType.equals(OriginalType.MAP)) {
        writeMap((ArrayWritable)value, groupType);
      } else {
        writeGroup((ArrayWritable) value, groupType);
      }
    } else {
      throw new RuntimeException("Field value is not an ArrayWritable object: " + type);
    }
  }

  /**
   * It writes a group type and all its values to the Parquet RecordConsumer.
   * This is used only for optional and required groups.
   * @param value ArrayWritable object that contains the group values
   * @param type Type that contains information about the group schema
   */
  private void writeGroup(final ArrayWritable value, final GroupType type) {
    recordConsumer.startGroup();
    writeGroupFields(value, type);
    recordConsumer.endGroup();
  }

  /**
   * It writes a map type and its key-pair values to the Parquet RecordConsumer.
   * This is called when the original type (MAP) is detected by writeValue()
   * @param value The list of map values that contains the repeated KEY_PAIR_VALUE group type
   * @param type Type that contains information about the group schema
   */
  private void writeMap(final ArrayWritable value, final GroupType type) {
    GroupType repeatedType = type.getType(0).asGroupType();
    ArrayWritable repeatedValue = (ArrayWritable)value.get()[0];

    recordConsumer.startGroup();
    recordConsumer.startField(repeatedType.getName(), 0);

    Writable[] map_values = repeatedValue.get();
    for (int record = 0; record < map_values.length; record++) {
      Writable key_value_pair = map_values[record];
      if (key_value_pair != null) {
        // Hive wraps a map key-pair into an ArrayWritable
        if (key_value_pair instanceof ArrayWritable) {
          writeGroup((ArrayWritable)key_value_pair, repeatedType);
        } else {
          throw new RuntimeException("Map key-value pair is not an ArrayWritable object on record " + record);
        }
      } else {
        throw new RuntimeException("Map key-value pair is null on record " + record);
      }
    }

    recordConsumer.endField(repeatedType.getName(), 0);
    recordConsumer.endGroup();
  }

  /**
   * It writes a list type and its array elements to the Parquet RecordConsumer.
   * This is called when the original type (LIST) is detected by writeValue()
   * @param array The list of array values that contains the repeated array group type
   * @param type Type that contains information about the group schema
   */
  private void writeArray(final ArrayWritable array, final GroupType type) {
    GroupType repeatedType = type.getType(0).asGroupType();
    ArrayWritable repeatedValue = (ArrayWritable)array.get()[0];

    recordConsumer.startGroup();
    recordConsumer.startField(repeatedType.getName(), 0);

    Writable[] array_values = repeatedValue.get();
    for (int record = 0; record < array_values.length; record++) {
      recordConsumer.startGroup();

      // Null values must be wrapped into startGroup/endGroup
      Writable element = array_values[record];
      if (element != null) {
        for (int i = 0; i < type.getFieldCount(); i++) {
          Type fieldType = repeatedType.getType(i);
          String fieldName = fieldType.getName();

          recordConsumer.startField(fieldName, i);
          writeValue(element, fieldType);
          recordConsumer.endField(fieldName, i);
        }
      }

      recordConsumer.endGroup();
    }

    recordConsumer.endField(repeatedType.getName(), 0);
    recordConsumer.endGroup();
  }

  /**
   * It writes the primitive value to the Parquet RecordConsumer.
   * @param value The writable object that contains the primitive value.
   */
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
    } else if (value instanceof HiveDecimalWritable) {
      throw new UnsupportedOperationException("HiveDecimalWritable writing not implemented");
    } else if (value instanceof BytesWritable) {
      recordConsumer.addBinary((Binary.fromByteArray(((BytesWritable) value).getBytes())));
    } else if (value instanceof TimestampWritable) {
      Timestamp ts = ((TimestampWritable) value).getTimestamp();
      NanoTime nt = NanoTimeUtils.getNanoTime(ts, false);
      nt.writeValue(recordConsumer);
    } else {
      throw new IllegalArgumentException("Unknown value type: " + value + " " + value.getClass());
    }
  }
}
