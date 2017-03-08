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
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetTableUtils;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.parquet.Strings;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

/**
 *
 * ETypeConverter is an easy way to set the converter for the right type.
 *
 */
public enum ETypeConverter {

  EDOUBLE_CONVERTER(Double.TYPE) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
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
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
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
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
      if (hiveTypeInfo != null && hiveTypeInfo.equals(TypeInfoFactory.doubleTypeInfo)) {
        return new PrimitiveConverter() {
          @Override
          public void addFloat(final float value) {
            parent.set(index, new DoubleWritable((double) value));
          }
        };
      } else {
        return new PrimitiveConverter() {
          @Override
          public void addFloat(final float value) {
            parent.set(index, new FloatWritable(value));
          }
        };
      }
    }
  },
  EINT32_CONVERTER(Integer.TYPE) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index,
        final ConverterParent parent, TypeInfo hiveTypeInfo) {
      if (hiveTypeInfo != null) {
        switch (hiveTypeInfo.getTypeName()) {
        case serdeConstants.BIGINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addInt(final int value) {
              parent.set(index, new LongWritable((long) value));
            }
          };
        case serdeConstants.FLOAT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addInt(final int value) {
              parent.set(index, new FloatWritable((float) value));
            }
          };
        case serdeConstants.DOUBLE_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addInt(final int value) {
              parent.set(index, new DoubleWritable((float) value));
            }
          };
        }
      }
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
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
      if(hiveTypeInfo != null) {
        switch(hiveTypeInfo.getTypeName()) {
        case serdeConstants.FLOAT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addLong(final long value) {
              parent.set(index, new FloatWritable(value));
            }
          };
        case serdeConstants.DOUBLE_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addLong(final long value) {
              parent.set(index, new DoubleWritable(value));
            }
          };
        }
      }
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
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
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
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
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
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
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
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
      Map<String, String> metadata = parent.getMetadata();

      // This variable must be initialized only once to keep good read performance while doing conversion of timestamps values.
      final Calendar calendar;
      if (Strings.isNullOrEmpty(metadata.get(ParquetTableUtils.PARQUET_INT96_WRITE_ZONE_PROPERTY))) {
        // Local time should be used if timezone is not available.
        calendar = Calendar.getInstance();
      } else {
        calendar = Calendar.getInstance(TimeZone.getTimeZone(metadata.get(ParquetTableUtils.PARQUET_INT96_WRITE_ZONE_PROPERTY)));
      }

      return new BinaryConverter<TimestampWritable>(type, parent, index) {
        @Override
        protected TimestampWritable convert(Binary binary) {
          Timestamp ts = NanoTimeUtils.getTimestamp(NanoTime.fromBinary(binary), calendar);
          return new TimestampWritable(ts);
        }
      };
    }
  },
  EDATE_CONVERTER(DateWritable.class) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
      return new PrimitiveConverter() {
        @Override
        public void addInt(final int value) {
          parent.set(index, new DateWritable(value));
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

  abstract PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo);

  public static PrimitiveConverter getNewConverter(final PrimitiveType type, final int index,
                                                   final ConverterParent parent, TypeInfo hiveTypeInfo) {
    if (type.isPrimitive() && (type.asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT96))) {
      //TODO- cleanup once parquet support Timestamp type annotation.
      return ETypeConverter.ETIMESTAMP_CONVERTER.getConverter(type, index, parent, hiveTypeInfo);
    }
    if (OriginalType.DECIMAL == type.getOriginalType()) {
      return EDECIMAL_CONVERTER.getConverter(type, index, parent, hiveTypeInfo);
    } else if (OriginalType.UTF8 == type.getOriginalType()) {
      return ESTRING_CONVERTER.getConverter(type, index, parent, hiveTypeInfo);
    } else if (OriginalType.DATE == type.getOriginalType()) {
      return EDATE_CONVERTER.getConverter(type, index, parent, hiveTypeInfo);
    }

    Class<?> javaType = type.getPrimitiveTypeName().javaType;
    for (final ETypeConverter eConverter : values()) {
      if (eConverter.getType() == javaType) {
        return eConverter.getConverter(type, index, parent, hiveTypeInfo);
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
