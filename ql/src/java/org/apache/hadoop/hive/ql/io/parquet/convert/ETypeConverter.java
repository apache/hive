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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import com.google.common.base.MoreObjects;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.ParquetTimestampUtils;
import org.apache.hadoop.hive.common.type.CalendarUtils;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
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
          if (value >= ((ETypeConverter.isUnsignedInteger(type)) ? 0 :
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
        case serdeConstants.TIMESTAMP_TYPE_NAME:
        case serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME:
          if (type.getLogicalTypeAnnotation() instanceof TimestampLogicalTypeAnnotation) {
            TimestampLogicalTypeAnnotation logicalType =
                (TimestampLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
            return new PrimitiveConverter() {
              @Override
              public void addLong(final long value) {
                Timestamp timestamp =
                    ParquetTimestampUtils.getTimestamp(value, logicalType.getUnit(), logicalType.isAdjustedToUTC());
                parent.set(index, new TimestampWritableV2(timestamp));
              }
            };
          }
          throw new IllegalStateException("Cannot reliably convert INT64 value to timestamp without type annotation");
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
          if (value >= ((ETypeConverter.isUnsignedInteger(type)) ? 0 : Long.MIN_VALUE)) {
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
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent,
                                    TypeInfo hiveTypeInfo) {
      // If we have type information, we should return properly typed strings. However, there are a variety
      // of code paths that do not provide the typeInfo in those cases we default to Text. This idiom is also
      // followed by for example the BigDecimal converter in which if there is no type information,
      // it defaults to the widest representation
      if (hiveTypeInfo instanceof PrimitiveTypeInfo) {
        PrimitiveTypeInfo t = (PrimitiveTypeInfo) hiveTypeInfo;
        switch (t.getPrimitiveCategory()) {
          case CHAR:
            return new BinaryConverter<HiveCharWritable>(type, parent, index) {
              @Override
              protected HiveCharWritable convert(Binary binary) {
                return new HiveCharWritable(binary.getBytes(), ((CharTypeInfo) hiveTypeInfo).getLength());
              }
            };
          case VARCHAR:
            return new BinaryConverter<HiveVarcharWritable>(type, parent, index) {
              @Override
              protected HiveVarcharWritable convert(Binary binary) {
                return new HiveVarcharWritable(binary.getBytes(), ((VarcharTypeInfo) hiveTypeInfo).getLength());
              }
            };
        }
      }
      // STRING type
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
                  new HiveDecimalWritable(value.getBytes(), getScale(type));
              setValue(decimalWritable.doubleValue(), decimalWritable.floatValue());
            }

            @Override
            public void addInt(final int value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, getScale(type));
              setValue(hiveDecimal.doubleValue(), hiveDecimal.floatValue());
            }

            @Override
            public void addLong(final long value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, getScale(type));
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

            private int getScale(PrimitiveType type) {
              DecimalLogicalTypeAnnotation logicalType = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
              return logicalType.getScale();
            }
          };
        case serdeConstants.DOUBLE_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addBinary(Binary value) {
              HiveDecimalWritable decimalWritable =
                  new HiveDecimalWritable(value.getBytes(), getScale(type));
              parent.set(index, new DoubleWritable(decimalWritable.doubleValue()));
            }

            @Override
            public void addInt(final int value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, getScale(type));
              parent.set(index, new DoubleWritable(hiveDecimal.doubleValue()));
            }

            @Override
            public void addLong(final long value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, getScale(type));
              parent.set(index, new DoubleWritable(hiveDecimal.doubleValue()));
            }

            private int getScale(PrimitiveType type) {
              DecimalLogicalTypeAnnotation logicalType = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
              return logicalType.getScale();
            }
          };
        case serdeConstants.BIGINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addBinary(Binary value) {
              HiveDecimalWritable decimalWritable =
                  new HiveDecimalWritable(value.getBytes(), getScale(type));
              setValue(decimalWritable.doubleValue(), decimalWritable.longValue());
            }

            @Override
            public void addInt(final int value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, getScale(type));
              setValue(hiveDecimal.doubleValue(), hiveDecimal.longValue());
            }

            @Override
            public void addLong(final long value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, getScale(type));
              setValue(hiveDecimal.doubleValue(), hiveDecimal.longValue());
            }

            private void setValue(double doubleValue, long longValue) {
              if ((doubleValue >= minValue) && (doubleValue <= maxValue) && (doubleValue % 1 == 0)) {
                parent.set(index, new LongWritable(longValue));
              } else {
                parent.set(index, null);
              }
            }

            private int getScale(PrimitiveType type) {
              DecimalLogicalTypeAnnotation logicalType = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
              return logicalType.getScale();
            }
          };
        case serdeConstants.INT_TYPE_NAME:
        case serdeConstants.SMALLINT_TYPE_NAME:
        case serdeConstants.TINYINT_TYPE_NAME:
          return new PrimitiveConverter() {
            @Override
            public void addBinary(Binary value) {
              HiveDecimalWritable decimalWritable =
                  new HiveDecimalWritable(value.getBytes(), getScale(type));
              setValue(decimalWritable.doubleValue(), decimalWritable.intValue());
            }

            @Override
            public void addInt(final int value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, getScale(type));
              setValue(hiveDecimal.doubleValue(), hiveDecimal.intValue());
            }

            @Override
            public void addLong(final long value) {
              HiveDecimal hiveDecimal = HiveDecimal.create(value, getScale(type));
              setValue(hiveDecimal.doubleValue(), hiveDecimal.intValue());
            }

            private void setValue(double doubleValue, int intValue) {
              if ((doubleValue >= minValue) && (doubleValue <= maxValue) && (doubleValue % 1 == 0)) {
                parent.set(index, new IntWritable(intValue));
              } else {
                parent.set(index, null);
              }
            }

            private int getScale(PrimitiveType type) {
              DecimalLogicalTypeAnnotation logicalType = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
              return logicalType.getScale();
            }
          };
        case serdeConstants.CHAR_TYPE_NAME:
          return new BinaryConverterForDecimalType<HiveCharWritable>(type, parent, index, hiveTypeInfo) {
            @Override
            protected HiveCharWritable convert(Binary binary) {
              return new HiveCharWritable(convertToBytes(binary), ((CharTypeInfo) hiveTypeInfo).getLength());
            }
          };
        case serdeConstants.VARCHAR_TYPE_NAME:
          return new BinaryConverterForDecimalType<HiveVarcharWritable>(type, parent, index, hiveTypeInfo) {
            @Override
            protected HiveVarcharWritable convert(Binary binary) {
              return new HiveVarcharWritable(convertToBytes(binary), ((VarcharTypeInfo) hiveTypeInfo).getLength());
            }
          };
        case serdeConstants.STRING_TYPE_NAME:
          return new BinaryConverterForDecimalType<BytesWritable>(type, parent, index, hiveTypeInfo) {
            @Override
            protected BytesWritable convert(Binary binary) {
              return new BytesWritable(convertToBytes(binary));
            }
          };
        default:
          return new BinaryConverter<HiveDecimalWritable>(type, parent, index, hiveTypeInfo) {
            @Override
            protected HiveDecimalWritable convert(Binary binary) {
              DecimalLogicalTypeAnnotation logicalType = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
              return HiveDecimalUtils.enforcePrecisionScale(
                  new HiveDecimalWritable(binary.getBytes(), logicalType.getScale()),
                  (DecimalTypeInfo) hiveTypeInfo);
            }

            // TODO HIVE-27529 Add dictionary encoding support for parquet decimal types
            @Override
            public boolean hasDictionarySupport() {
              return false;
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
          DecimalLogicalTypeAnnotation logicalType =
              (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
          return new HiveDecimalWritable(binary.getBytes(), logicalType.getScale());
        }
      };
    }
  },
  EINT96_TIMESTAMP_CONVERTER(TimestampWritableV2.class) {
    @Override
    PrimitiveConverter getConverter(final PrimitiveType type, final int index, final ConverterParent parent, TypeInfo hiveTypeInfo) {
      if (hiveTypeInfo != null) {
        String typeName = TypeInfoUtils.getBaseName(hiveTypeInfo.getTypeName());
        switch (typeName) {
          case serdeConstants.BIGINT_TYPE_NAME:
            return new BinaryConverter<LongWritable>(type, parent, index) {
              @Override
              protected LongWritable convert(Binary binary) {
                Preconditions.checkArgument(binary.length() == 12, "Must be 12 bytes");
                ByteBuffer buf = binary.toByteBuffer();
                buf.order(ByteOrder.LITTLE_ENDIAN);
                long longVal = buf.getLong();
                return new LongWritable(longVal);
              }
            };
        }
      }
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
          String legacyConversion = metadata.get(DataWritableWriteSupport.WRITER_ZONE_CONVERSION_LEGACY);
          assert legacyConversion != null;
          ZoneId targetZone = skipConversion ? ZoneOffset.UTC : MoreObjects
              .firstNonNull(DataWritableReadSupport.getWriterTimeZoneId(metadata), TimeZone.getDefault().toZoneId());
          Timestamp ts = NanoTimeUtils.getTimestamp(nt, targetZone, Boolean.parseBoolean(legacyConversion));
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
          Map<String, String> metadata = parent.getMetadata();
          Boolean skipProlepticConversion = DataWritableReadSupport.getWriterDateProleptic(metadata);
          if (skipProlepticConversion == null) {
            skipProlepticConversion = Boolean.parseBoolean(
                metadata.get(HiveConf.ConfVars.HIVE_PARQUET_DATE_PROLEPTIC_GREGORIAN_DEFAULT.varname));
          }
          parent.set(index,
              new DateWritableV2(skipProlepticConversion ? value : CalendarUtils.convertDateToProleptic(value)));
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
                                                   final ConverterParent parent, final TypeInfo hiveTypeInfo) {
    if (type.isPrimitive() && (type.asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT96))) {
      return EINT96_TIMESTAMP_CONVERTER.getConverter(type, index, parent, hiveTypeInfo);
    }
    if (type.getLogicalTypeAnnotation() != null) {
      Optional<PrimitiveConverter> converter = type.getLogicalTypeAnnotation()
          .accept(new LogicalTypeAnnotationVisitor<PrimitiveConverter>() {
            @Override
            public Optional<PrimitiveConverter> visit(DecimalLogicalTypeAnnotation logicalTypeAnnotation) {
              return Optional.of(EDECIMAL_CONVERTER.getConverter(type, index, parent, hiveTypeInfo));
            }

            @Override
            public Optional<PrimitiveConverter> visit(StringLogicalTypeAnnotation logicalTypeAnnotation) {
              return Optional.of(ESTRING_CONVERTER.getConverter(type, index, parent, hiveTypeInfo));
            }

            @Override
            public Optional<PrimitiveConverter> visit(DateLogicalTypeAnnotation logicalTypeAnnotation) {
              return Optional.of(EDATE_CONVERTER.getConverter(type, index, parent, hiveTypeInfo));
            }

            @Override
            public Optional<PrimitiveConverter> visit(TimestampLogicalTypeAnnotation logicalTypeAnnotation) {
              TypeInfo info = hiveTypeInfo == null ? TypeInfoFactory.timestampTypeInfo : hiveTypeInfo;
              return Optional.of(EINT64_CONVERTER.getConverter(type, index, parent, info));
            }
          });

      if (converter.isPresent()) {
        return converter.get();
      }
    }

    Class<?> javaType = type.getPrimitiveTypeName().javaType;
    for (final ETypeConverter eConverter : values()) {
      if (eConverter.getType() == javaType) {
        return eConverter.getConverter(type, index, parent, hiveTypeInfo);
      }
    }

    throw new IllegalArgumentException("Converter not found ... for type : " + type);
  }

  public static boolean isUnsignedInteger(final PrimitiveType type) {
    if (type.getLogicalTypeAnnotation() != null) {
      Optional<Boolean> isUnsignedInteger = type.getLogicalTypeAnnotation()
          .accept(new LogicalTypeAnnotationVisitor<Boolean>() {
            @Override public Optional<Boolean> visit(
                LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
              return Optional.of(!intLogicalType.isSigned());
            }
          });
      if (isUnsignedInteger.isPresent()) {
        return isUnsignedInteger.get();
      }
    }
    return false;
  }

  private static long getMinValue(final PrimitiveType type, String typeName, long defaultValue) {
    if(isUnsignedInteger(type)) {
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

  public abstract static class BinaryConverterForDecimalType<T extends Writable> extends BinaryConverter<T> {

    public BinaryConverterForDecimalType(PrimitiveType type, ConverterParent parent, int index, TypeInfo hiveTypeInfo) {
      super(type, parent, index, hiveTypeInfo);
    }

    protected byte[] convertToBytes(Binary binary) {
      DecimalLogicalTypeAnnotation logicalType = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
      return HiveDecimalUtils.enforcePrecisionScale(new HiveDecimalWritable(binary.getBytes(), logicalType.getScale()),
                      new DecimalTypeInfo(logicalType.getPrecision(), logicalType.getScale())).toString().getBytes();
    }
  }
}
