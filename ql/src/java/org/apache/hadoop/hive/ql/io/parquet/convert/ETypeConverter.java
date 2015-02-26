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
package org.apache.hadoop.hive.ql.io.parquet.convert;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import parquet.column.Dictionary;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;

/**
 *
 * ETypeConverter is an easy way to set the converter for the right type.
 *
 */
public enum ETypeConverter {

  EDOUBLE_CONVERTER(Double.TYPE) {
    @Override

    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent) {
      return new PrimitiveConverter() {
        @Override
        public void addDouble(final double value) {
          parent.set(index, new DoubleWritable(value));
        }
      };
    }
  },
  EBOOLEAN_CONVERTER(Boolean.TYPE) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent) {
      return new PrimitiveConverter() {
        @Override
        public void addBoolean(final boolean value) {
          parent.set(index, new BooleanWritable(value));
        }
      };
    }
  },
  EFLOAT_CONVERTER(Float.TYPE) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent) {
      return new PrimitiveConverter() {
        @Override
        public void addFloat(final float value) {
          parent.set(index, new FloatWritable(value));
        }
      };
    }
  },
  EINT32_CONVERTER(Integer.TYPE) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent) {
      return new PrimitiveConverter() {
        @Override
        public void addInt(final int value) {
          parent.set(index, new IntWritable(value));
        }
      };
    }
  },
  EINT64_CONVERTER(Long.TYPE) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent) {
      return new PrimitiveConverter() {
        @Override
        public void addLong(final long value) {
          parent.set(index, new LongWritable(value));
        }
      };
    }
  },
  EBINARY_CONVERTER(Binary.class) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent) {
      return new BinaryConverter<BytesWritable>(type, parent, index) {
        @Override
        protected BytesWritable convert(Binary binary) {
          return new BytesWritable(binary.getBytes());
        }
      };
    }
  },
  ESTRING_CONVERTER(String.class) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent) {
      return new BinaryConverter<Text>(type, parent, index) {
        @Override
        protected Text convert(Binary binary) {
          return new Text(binary.getBytes());
        }
      };
    }
  },
  EDECIMAL_CONVERTER(BigDecimal.class) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent) {
      return new BinaryConverter<HiveDecimalWritable>(type, parent, index) {
        @Override
        protected HiveDecimalWritable convert(Binary binary) {
          return new HiveDecimalWritable(binary.getBytes(), type.getDecimalMetadata().getScale());
        }
      };
    }
  },
  ETIMESTAMP_CONVERTER(TimestampWritable.class) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent) {
      return new BinaryConverter<TimestampWritable>(type, parent, index) {
        @Override
        protected TimestampWritable convert(Binary binary) {
          NanoTime nt = NanoTime.fromBinary(binary);
          Map<String, String> metadata = parent.getMetadata();
          //Current Hive parquet timestamp implementation stores it in UTC, but other components do not do that.
          //If this file written by current Hive implementation itself, we need to do the reverse conversion, else skip the conversion.
          boolean skipConversion = Boolean.valueOf(metadata.get(HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION.varname));
          Timestamp ts = NanoTimeUtils.getTimestamp(nt, skipConversion);
          return new TimestampWritable(ts);
        }
      };
    }
  };

  final Class<?> _type;

  private ETypeConverter(final Class<?> type) {
    this._type = type;
  }

  private Class<?> getType() {
    return _type;
  }

  abstract PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent);

  public static PrimitiveConverter getNewConverter(final PrimitiveType type, final int index,
      final ConverterParent parent) {
    if (type.isPrimitive() && (type.asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT96))) {
      //TODO- cleanup once parquet support Timestamp type annotation.
      return ETypeConverter.ETIMESTAMP_CONVERTER.getConverter(type, index, parent);
    }
    if (OriginalType.DECIMAL == type.getOriginalType()) {
      return EDECIMAL_CONVERTER.getConverter(type, index, parent);
    } else if (OriginalType.UTF8 == type.getOriginalType()) {
      return ESTRING_CONVERTER.getConverter(type, index, parent);
    }

    Class<?> javaType = type.getPrimitiveTypeName().javaType;
    for (final ETypeConverter eConverter : values()) {
      if (eConverter.getType() == javaType) {
        return eConverter.getConverter(type, index, parent);
      }
    }

    throw new IllegalArgumentException("Converter not found ... for type : " + type);
  }

  public abstract static class BinaryConverter<T extends Writable> extends PrimitiveConverter {
    protected final PrimitiveType type;
    private final ConverterParent parent;
    private final int index;
    private ArrayList<T> lookupTable;

    public BinaryConverter(PrimitiveType type, ConverterParent parent, int index) {
      this.type = type;
      this.parent = parent;
      this.index = index;
    }

    protected abstract T convert(Binary binary);

    @Override
    public boolean hasDictionarySupport() {
      return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      int length = dictionary.getMaxId() + 1;
      lookupTable = new ArrayList<T>();
      for (int i = 0; i < length; i++) {
        lookupTable.add(convert(dictionary.decodeToBinary(i)));
      }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      parent.set(index, lookupTable.get(dictionaryId));
    }

    @Override
    public void addBinary(Binary value) {
      parent.set(index, convert(value));
    }
  }

}
