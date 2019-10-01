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
package org.apache.hadoop.hive.ql.io.parquet.convert;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

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
      if (hiveTypeInfo != null) {
        String typeName = TypeInfoUtils.getBaseName(hiveTypeInfo.getTypeName());
        final double minValue = getMinValue(typeName, Double.MIN_VALUE);
        final double maxValue = getMaxValue(typeName, Double.MAX_VALUE);

        switch (typeName) {
        case serdeConstants.FLOAT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addDouble(final double value) {
              double absValue = (value < 0) ? (value * -1) : value;
              int exponent = Math.getExponent(value);
              if ((absValue >= minValue) && (absValue <= maxValue) &&
                  (exponent <= Float.MAX_EXPONENT) && (exponent >= Float.MIN_EXPONENT)) {
                parent.set(index, new FloatWritable((float) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.DECIMAL_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addDouble(final double value) {
              HiveDecimalWritable decimalWritable = new HiveDecimalWritable();
              decimalWritable.setFromDouble(value);
              parent.set(index, HiveDecimalUtils
                  .enforcePrecisionScale(decimalWritable, (DecimalTypeInfo) hiveTypeInfo));
            }
          };
        case serdeConstants.BIGINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addDouble(final double value) {
              if ((value >= minValue) && (value <= maxValue) && (value % 1 == 0)) {
                parent.set(index, new LongWritable((long) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.INT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addDouble(final double value) {
              if ((value >= minValue) && (value <= maxValue) && (value % 1 == 0)) {
                parent.set(index, new IntWritable((int) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.SMALLINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addDouble(final double value) {
              if ((value >= minValue) && (value <= maxValue) && (value % 1 == 0)) {
                parent.set(index, new IntWritable((int) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.TINYINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addDouble(final double value) {
              if ((value >= minValue) && (value <= maxValue) && (value % 1 == 0)) {
                parent.set(index, new IntWritable((int) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        default:
          return new PrimitiveConverter() {
            @Override
            public void addDouble(final double value) {
              parent.set(index, new DoubleWritable(value));
            }
          };
        }
      }
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
      if (hiveTypeInfo != null) {
        String typeName = TypeInfoUtils.getBaseName(hiveTypeInfo.getTypeName());
        final double minValue = getMinValue(typeName, Double.MIN_VALUE);
        final double maxValue = getMaxValue(typeName, Double.MAX_VALUE);

        switch (typeName) {
        case serdeConstants.DOUBLE_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addFloat(final float value) {
              parent.set(index, new DoubleWritable(value));
            }
          };
        case serdeConstants.DECIMAL_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addFloat(final float value) {
              HiveDecimalWritable decimalWritable = new HiveDecimalWritable();
              decimalWritable.setFromDouble(value);
              parent.set(index, HiveDecimalUtils
                  .enforcePrecisionScale(decimalWritable, (DecimalTypeInfo) hiveTypeInfo));
            }
          };
        case serdeConstants.BIGINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addFloat(final float value) {
              if ((value >= minValue) && (value <= maxValue) && (value % 1 == 0)) {
                parent.set(index, new LongWritable((long) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.INT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addFloat(final float value) {
              if ((value >= minValue) && (value <= maxValue) && (value % 1 == 0)) {
                parent.set(index, new IntWritable((int) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.SMALLINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addFloat(final float value) {
              if ((value >= minValue) && (value <= maxValue) && (value % 1 == 0)) {
                parent.set(index, new IntWritable((int) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.TINYINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addFloat(final float value) {
              if ((value >= minValue) && (value <= maxValue) && (value % 1 == 0)) {
                parent.set(index, new IntWritable((int) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        default:
          return new PrimitiveConverter() {
            @Override
            public void addFloat(final float value) {
              parent.set(index, new FloatWritable(value));
            }
          };
        }
      }

      return new PrimitiveConverter() {
        @Override public void addFloat(final float value) {
          parent.set(index, new FloatWritable(value));
        }
      };
    }
  },
  EINT32_CONVERTER(Integer.TYPE) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index,
        final ConverterParent parent, TypeInfo hiveTypeInfo) {
      if (hiveTypeInfo != null) {
        String typeName = TypeInfoUtils.getBaseName(hiveTypeInfo.getTypeName());
        final long minValue = getMinValue(type, typeName, Integer.MIN_VALUE);
        final long maxValue = getMaxValue(typeName, Integer.MAX_VALUE);

        switch (typeName) {
        case serdeConstants.BIGINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addInt(final int value) {
              if (value >= minValue) {
                parent.set(index, new LongWritable((long) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.FLOAT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addInt(final int value) {
              if (value >= minValue) {
                parent.set(index, new FloatWritable((float) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.DOUBLE_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addInt(final int value) {
              if (value >= minValue) {
                parent.set(index, new DoubleWritable((float) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.DECIMAL_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addInt(final int value) {
              if (value >= minValue) {
                parent.set(index, HiveDecimalUtils
                    .enforcePrecisionScale(new HiveDecimalWritable(value),
                        (DecimalTypeInfo) hiveTypeInfo));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.SMALLINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addInt(final int value) {
              if ((value >= minValue) && (value <= maxValue)) {
                parent.set(index, new IntWritable((int) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.TINYINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addInt(final int value) {
              if ((value >= minValue) && (value <= maxValue)) {
                parent.set(index, new IntWritable((int) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        default:
          return new PrimitiveConverter() {
            @Override
            public void addInt(final int value) {
              if ((value >= minValue) && (value <= maxValue)) {
                parent.set(index, new IntWritable(value));
              } else {
                parent.set(index, null);
              }
            }
          };
        }
      }
      return new PrimitiveConverter() {
        @Override
        public void addInt(final int value) {
          if (value >= ((OriginalType.UINT_8 == type.getOriginalType() ||
                          OriginalType.UINT_16 == type.getOriginalType() ||
                          OriginalType.UINT_32 == type.getOriginalType() ||
                          OriginalType.UINT_64 == type.getOriginalType()) ? 0 :
              Integer.MIN_VALUE)) {
            parent.set(index, new IntWritable(value));
          } else {
            parent.set(index, null);
          }
        }
      };
    }
  },
  EINT64_CONVERTER(Long.TYPE) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index,
        final ConverterParent parent, TypeInfo hiveTypeInfo) {
      if (hiveTypeInfo != null) {
        String typeName = TypeInfoUtils.getBaseName(hiveTypeInfo.getTypeName());
        final long minValue = getMinValue(type, typeName, Long.MIN_VALUE);
        final long maxValue = getMaxValue(typeName, Long.MAX_VALUE);

        switch (typeName) {
        case serdeConstants.FLOAT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addLong(final long value) {
              if (value >= minValue) {
                parent.set(index, new FloatWritable(value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.DOUBLE_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addLong(final long value) {
              if (value >= minValue) {
                parent.set(index, new DoubleWritable(value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.DECIMAL_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addLong(long value) {
              if (value >= minValue) {
                parent.set(index, HiveDecimalUtils
                    .enforcePrecisionScale(new HiveDecimalWritable(value),
                        (DecimalTypeInfo) hiveTypeInfo));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.INT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addLong(long value) {
              if ((value >= minValue) && (value <= maxValue)) {
                parent.set(index, new IntWritable((int) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.SMALLINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addLong(long value) {
              if ((value >= minValue) && (value <= maxValue)) {
                parent.set(index, new IntWritable((int) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.TINYINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addLong(long value) {
              if ((value >= minValue) && (value <= maxValue)) {
                parent.set(index, new IntWritable((int) value));
              } else {
                parent.set(index, null);
              }
            }
          };
        default:
          return new PrimitiveConverter() {
            @Override
            public void addLong(final long value) {
              if (value >= minValue) {
                parent.set(index, new LongWritable(value));
              } else {
                parent.set(index, null);
              }
            }
          };
        }
      }
      return new PrimitiveConverter() {
        @Override
        public void addLong(final long value) {
          if (value >= ((OriginalType.UINT_8 == type.getOriginalType() ||
                         OriginalType.UINT_16 == type.getOriginalType() ||
                         OriginalType.UINT_32 == type.getOriginalType() ||
                         OriginalType.UINT_64 == type.getOriginalType()) ? 0 : Long.MIN_VALUE)) {
            parent.set(index, new LongWritable(value));
          } else {
            parent.set(index, null);
          }
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
      if (hiveTypeInfo != null) {
        String typeName = TypeInfoUtils.getBaseName(hiveTypeInfo.getTypeName());
        final double minValue = getMinValue(typeName, Double.MIN_VALUE);
        final double maxValue = getMaxValue(typeName, Double.MAX_VALUE);

        switch (typeName) {
        case serdeConstants.FLOAT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addBinary(Binary value) {
              HiveDecimalWritable decimalWritable =
                  new HiveDecimalWritable(value.getBytes(), type.getDecimalMetadata().getScale());
              setValue(decimalWritable.doubleValue(), decimalWritable.floatValue());
            }

            @Override
            public void addInt(final int value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, type.getDecimalMetadata().getScale());
              setValue(hiveDecimal.doubleValue(), hiveDecimal.floatValue());
            }

            @Override
            public void addLong(final long value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, type.getDecimalMetadata().getScale());
              setValue(hiveDecimal.doubleValue(), hiveDecimal.floatValue());
            }

            private void setValue(double doubleValue, float floatValue) {
              double absDoubleValue = (doubleValue < 0) ? (doubleValue * -1) : doubleValue;
              if (((absDoubleValue >= minValue) && (absDoubleValue <= maxValue)) || absDoubleValue == 0d) {
                parent.set(index, new FloatWritable(floatValue));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.DOUBLE_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addBinary(Binary value) {
              HiveDecimalWritable decimalWritable =
                  new HiveDecimalWritable(value.getBytes(), type.getDecimalMetadata().getScale());
              parent.set(index, new DoubleWritable(decimalWritable.doubleValue()));
            }

            @Override
            public void addInt(final int value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, type.getDecimalMetadata().getScale());
              parent.set(index, new DoubleWritable(hiveDecimal.doubleValue()));
            }

            @Override
            public void addLong(final long value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, type.getDecimalMetadata().getScale());
              parent.set(index, new DoubleWritable(hiveDecimal.doubleValue()));
            }
          };
        case serdeConstants.BIGINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addBinary(Binary value) {
              HiveDecimalWritable decimalWritable =
                  new HiveDecimalWritable(value.getBytes(), type.getDecimalMetadata().getScale());
              setValue(decimalWritable.doubleValue(), decimalWritable.longValue());
            }

            @Override
            public void addInt(final int value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, type.getDecimalMetadata().getScale());
              setValue(hiveDecimal.doubleValue(), hiveDecimal.longValue());
            }

            @Override
            public void addLong(final long value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, type.getDecimalMetadata().getScale());
              setValue(hiveDecimal.doubleValue(), hiveDecimal.longValue());
            }

            private void setValue(double doubleValue, long longValue) {
              if ((doubleValue >= minValue) && (doubleValue <= maxValue) && (doubleValue % 1 == 0)) {
                parent.set(index, new LongWritable(longValue));
              } else {
                parent.set(index, null);
              }
            }
          };
        case serdeConstants.INT_TYPE_NAME:
        case serdeConstants.SMALLINT_TYPE_NAME:
        case serdeConstants.TINYINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addBinary(Binary value) {
              HiveDecimalWritable decimalWritable =
                  new HiveDecimalWritable(value.getBytes(), type.getDecimalMetadata().getScale());
              setValue(decimalWritable.doubleValue(), decimalWritable.intValue());
            }

            @Override
            public void addInt(final int value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, type.getDecimalMetadata().getScale());
              setValue(hiveDecimal.doubleValue(), hiveDecimal.intValue());
            }

            @Override
            public void addLong(final long value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, type.getDecimalMetadata().getScale());
              setValue(hiveDecimal.doubleValue(), hiveDecimal.intValue());
            }

            private void setValue(double doubleValue, int intValue) {
              if ((doubleValue >= minValue) && (doubleValue <= maxValue) && (doubleValue % 1 == 0)) {
                parent.set(index, new IntWritable(intValue));
              } else {
                parent.set(index, null);
              }
            }
          };
        default:
          return new BinaryConverter<HiveDecimalWritable>(type, parent, index, hiveTypeInfo) {
            @Override
            protected HiveDecimalWritable convert(Binary binary) {
              return HiveDecimalUtils.enforcePrecisionScale(
                  new HiveDecimalWritable(binary.getBytes(), type.getDecimalMetadata().getScale()),
                  (DecimalTypeInfo) hiveTypeInfo);
            }

            @Override
            public void addInt(final int value) {
              addDecimal(value);
            }

            @Override
            public void addLong(final long value) {
              addDecimal(value);
            }

            private void addDecimal(long value) {
              DecimalTypeInfo decimalInfo = (DecimalTypeInfo) hiveTypeInfo;
              HiveDecimal hiveDecimal = HiveDecimal.create(value, decimalInfo.scale());
              HiveDecimalWritable result = HiveDecimalUtils.enforcePrecisionScale(new HiveDecimalWritable(hiveDecimal),
                  (DecimalTypeInfo) hiveTypeInfo);
              parent.set(index, result);
            }
          };
        }
      }
      return new BinaryConverter<HiveDecimalWritable>(type, parent, index) {
        @Override
        protected HiveDecimalWritable convert(Binary binary) {
          return new HiveDecimalWritable(binary.getBytes(), type.getDecimalMetadata().getScale());
        }
      };
    }
  },
  ETIMESTAMP_CONVERTER(TimestampWritableV2.class) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
      return new BinaryConverter<TimestampWritableV2>(type, parent, index) {
        @Override
        protected TimestampWritableV2 convert(Binary binary) {
          NanoTime nt = NanoTime.fromBinary(binary);
          Map<String, String> metadata = parent.getMetadata();
          // Current Hive parquet timestamp implementation stores timestamps in UTC, but other
          // components do not. In this case we skip timestamp conversion.
          // If this file is written by a version of hive before HIVE-21290, file metadata will
          // not contain the writer timezone, so we convert the timestamp to the system (reader)
          // time zone.
          // If file is written by current Hive implementation, we convert timestamps to the writer
          // time zone in order to emulate time zone agnostic behavior.
          boolean skipConversion = Boolean.parseBoolean(
              metadata.get(HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION.varname));
          Timestamp ts = NanoTimeUtils.getTimestamp(nt, skipConversion,
              DataWritableReadSupport.getWriterTimeZoneId(metadata));
          return new TimestampWritableV2(ts);
        }
      };
    }
  },
  EDATE_CONVERTER(DateWritableV2.class) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
      return new PrimitiveConverter() {
        @Override
        public void addInt(final int value) {
          parent.set(index, new DateWritableV2(value));
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

  private static long getMinValue(final PrimitiveType type, String typeName, long defaultValue) {
    if (OriginalType.UINT_8 == type.getOriginalType() ||
        OriginalType.UINT_16 == type.getOriginalType() ||
        OriginalType.UINT_32 == type.getOriginalType() ||
        OriginalType.UINT_64 == type.getOriginalType()) {
      return 0;
    } else {
      switch (typeName) {
      case serdeConstants.INT_TYPE_NAME:
        return Integer.MIN_VALUE;
      case serdeConstants.SMALLINT_TYPE_NAME:
        return Short.MIN_VALUE;
      case serdeConstants.TINYINT_TYPE_NAME:
        return Byte.MIN_VALUE;
      default:
        return defaultValue;
      }
    }
  }

  private static long getMaxValue(String typeName, long defaultValue) {
    switch (typeName) {
    case serdeConstants.INT_TYPE_NAME:
      return Integer.MAX_VALUE;
    case serdeConstants.SMALLINT_TYPE_NAME:
      return Short.MAX_VALUE;
    case serdeConstants.TINYINT_TYPE_NAME:
      return Byte.MAX_VALUE;
    default:
      return defaultValue;
    }
  }

  private static double getMinValue(String typeName, double defaultValue) {
    switch (typeName) {
    case serdeConstants.BIGINT_TYPE_NAME:
      return (double) Long.MIN_VALUE;
    case serdeConstants.INT_TYPE_NAME:
      return (double) Integer.MIN_VALUE;
    case serdeConstants.SMALLINT_TYPE_NAME:
      return (double) Short.MIN_VALUE;
    case serdeConstants.TINYINT_TYPE_NAME:
      return (double) Byte.MIN_VALUE;
    case serdeConstants.FLOAT_TYPE_NAME:
      return (double) Float.MIN_VALUE;
    default:
      return defaultValue;
    }
  }

  private static double getMaxValue(String typeName, double defaultValue) {
    switch (typeName) {
    case serdeConstants.BIGINT_TYPE_NAME:
      return (double) Long.MAX_VALUE;
    case serdeConstants.INT_TYPE_NAME:
      return (double) Integer.MAX_VALUE;
    case serdeConstants.SMALLINT_TYPE_NAME:
      return (double) Short.MAX_VALUE;
    case serdeConstants.TINYINT_TYPE_NAME:
      return (double) Byte.MAX_VALUE;
    case serdeConstants.FLOAT_TYPE_NAME:
      return (double) Float.MAX_VALUE;
    default:
      return defaultValue;
    }
  }

  public abstract static class BinaryConverter<T extends Writable> extends PrimitiveConverter {
    protected final PrimitiveType type;
    private final ConverterParent parent;
    private final int index;
    private final TypeInfo hiveTypeInfo;
    private ArrayList<T> lookupTable;

    public BinaryConverter(PrimitiveType type, ConverterParent parent, int index,
        TypeInfo hiveTypeInfo) {
      this.type = type;
      this.parent = parent;
      this.index = index;
      this.hiveTypeInfo = hiveTypeInfo;
    }

    public BinaryConverter(PrimitiveType type, ConverterParent parent, int index) {
      this(type, parent, index, null);
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
