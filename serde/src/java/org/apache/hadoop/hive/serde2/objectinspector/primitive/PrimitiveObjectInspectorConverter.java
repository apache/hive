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

package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * PrimitiveObjectInspectorConverter.
 *
 */
public class PrimitiveObjectInspectorConverter {

  /**
   * A converter for the byte type.
   */
  public static class BooleanConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableBooleanObjectInspector outputOI;
    Object r;

    public BooleanConverter(PrimitiveObjectInspector inputOI,
        SettableBooleanObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      r = outputOI.create(false);
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getBoolean(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the byte type.
   */
  public static class ByteConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableByteObjectInspector outputOI;
    Object r;

    public ByteConverter(PrimitiveObjectInspector inputOI,
        SettableByteObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      r = outputOI.create((byte) 0);
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getByte(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the short type.
   */
  public static class ShortConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableShortObjectInspector outputOI;
    Object r;

    public ShortConverter(PrimitiveObjectInspector inputOI,
        SettableShortObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      r = outputOI.create((short) 0);
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getShort(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the int type.
   */
  public static class IntConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableIntObjectInspector outputOI;
    Object r;

    public IntConverter(PrimitiveObjectInspector inputOI,
        SettableIntObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      r = outputOI.create(0);
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getInt(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the long type.
   */
  public static class LongConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableLongObjectInspector outputOI;
    Object r;

    public LongConverter(PrimitiveObjectInspector inputOI,
        SettableLongObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      r = outputOI.create(0);
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getLong(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the float type.
   */
  public static class FloatConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableFloatObjectInspector outputOI;
    Object r;

    public FloatConverter(PrimitiveObjectInspector inputOI,
        SettableFloatObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      r = outputOI.create(0);
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getFloat(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the double type.
   */
  public static class DoubleConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableDoubleObjectInspector outputOI;
    Object r;

    public DoubleConverter(PrimitiveObjectInspector inputOI,
        SettableDoubleObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      r = outputOI.create(0);
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getDouble(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  public static class TimestampConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableTimestampObjectInspector outputOI;
    Object r;

    public TimestampConverter(PrimitiveObjectInspector inputOI,
        SettableTimestampObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      r = outputOI.create(new Timestamp(0));
    }

    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      return outputOI.set(r, PrimitiveObjectInspectorUtils.getTimestamp(input,
          inputOI));
    }
  }

  public static class HiveDecimalConverter implements Converter {

    PrimitiveObjectInspector inputOI;
    SettableHiveDecimalObjectInspector outputOI;
    Object r;

    public HiveDecimalConverter(PrimitiveObjectInspector inputOI,
        SettableHiveDecimalObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      this.r = outputOI.create(HiveDecimal.ZERO);
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      return outputOI.set(r, PrimitiveObjectInspectorUtils.getHiveDecimal(input,
          inputOI));
    }

  }

  public static class BinaryConverter implements Converter{

    PrimitiveObjectInspector inputOI;
    SettableBinaryObjectInspector outputOI;
    Object r;

    public BinaryConverter(PrimitiveObjectInspector inputOI,
        SettableBinaryObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      r = outputOI.create(new byte[]{});
    }

    @Override
    public Object convert(Object input) {
      return outputOI.set(r, PrimitiveObjectInspectorUtils.getBinary(input,
          inputOI));
    }

  }

  /**
   * A helper class to convert any primitive to Text.
   */
  public static class TextConverter implements Converter {
    private final PrimitiveObjectInspector inputOI;
    private final Text t = new Text();
    private final ByteStream.Output out = new ByteStream.Output();

    private static byte[] trueBytes = {'T', 'R', 'U', 'E'};
    private static byte[] falseBytes = {'F', 'A', 'L', 'S', 'E'};

    public TextConverter(PrimitiveObjectInspector inputOI) {
      // The output ObjectInspector is writableStringObjectInspector.
      this.inputOI = inputOI;
    }

    public Text convert(Object input) {
      if (input == null) {
        return null;
      }

      switch (inputOI.getPrimitiveCategory()) {
      case VOID:
        return null;
      case BOOLEAN:
        t.set(((BooleanObjectInspector) inputOI).get(input) ? trueBytes
            : falseBytes);
        return t;
      case BYTE:
        out.reset();
        LazyInteger.writeUTF8NoException(out, ((ByteObjectInspector) inputOI).get(input));
        t.set(out.getData(), 0, out.getCount());
        return t;
      case SHORT:
        out.reset();
        LazyInteger.writeUTF8NoException(out, ((ShortObjectInspector) inputOI).get(input));
        t.set(out.getData(), 0, out.getCount());
        return t;
      case INT:
        out.reset();
        LazyInteger.writeUTF8NoException(out, ((IntObjectInspector) inputOI).get(input));
        t.set(out.getData(), 0, out.getCount());
        return t;
      case LONG:
        out.reset();
        LazyLong.writeUTF8NoException(out, ((LongObjectInspector) inputOI).get(input));
        t.set(out.getData(), 0, out.getCount());
        return t;
      case FLOAT:
        t.set(String.valueOf(((FloatObjectInspector) inputOI).get(input)));
        return t;
      case DOUBLE:
        t.set(String.valueOf(((DoubleObjectInspector) inputOI).get(input)));
        return t;
      case STRING:
        if (inputOI.preferWritable()) {
          t.set(((StringObjectInspector) inputOI).getPrimitiveWritableObject(input));
        } else {
          t.set(((StringObjectInspector) inputOI).getPrimitiveJavaObject(input));
        }
        return t;
      case TIMESTAMP:
        t.set(((TimestampObjectInspector) inputOI)
            .getPrimitiveWritableObject(input).toString());
        return t;
      case BINARY:
        t.set(((BinaryObjectInspector) inputOI).getPrimitiveWritableObject(input).getBytes());
        return t;
      case DECIMAL:
        t.set(((HiveDecimalObjectInspector) inputOI).getPrimitiveWritableObject(input).toString());
        return t;
      default:
        throw new RuntimeException("Hive 2 Internal error: type = " + inputOI.getTypeName());
      }
    }
  }

  /**
   * A helper class to convert any primitive to String.
   */
  public static class StringConverter implements Converter {
    PrimitiveObjectInspector inputOI;

    public StringConverter(PrimitiveObjectInspector inputOI) {
      // The output ObjectInspector is writableStringObjectInspector.
      this.inputOI = inputOI;
    }

    @Override
    public Object convert(Object input) {
      return PrimitiveObjectInspectorUtils.getString(input, inputOI);
    }
  }

}
