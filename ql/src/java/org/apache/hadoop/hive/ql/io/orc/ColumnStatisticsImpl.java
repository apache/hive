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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.BytesWritable;

class ColumnStatisticsImpl implements ColumnStatistics {

  private static final class BooleanStatisticsImpl extends ColumnStatisticsImpl
      implements BooleanColumnStatistics {
    private long trueCount = 0;

    BooleanStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.BucketStatistics bkt = stats.getBucketStatistics();
      trueCount = bkt.getCount(0);
    }

    BooleanStatisticsImpl() {
    }

    @Override
    void reset() {
      super.reset();
      trueCount = 0;
    }

    @Override
    void updateBoolean(boolean value) {
      if (value) {
        trueCount += 1;
      }
    }

    @Override
    void merge(ColumnStatisticsImpl other) {
      super.merge(other);
      BooleanStatisticsImpl bkt = (BooleanStatisticsImpl) other;
      trueCount += bkt.trueCount;
    }

    @Override
    OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.BucketStatistics.Builder bucket =
        OrcProto.BucketStatistics.newBuilder();
      bucket.addCount(trueCount);
      builder.setBucketStatistics(bucket);
      return builder;
    }

    @Override
    public long getFalseCount() {
      return getNumberOfValues() - trueCount;
    }

    @Override
    public long getTrueCount() {
      return trueCount;
    }

    @Override
    public String toString() {
      return super.toString() + " true: " + trueCount;
    }
  }

  private static final class IntegerStatisticsImpl extends ColumnStatisticsImpl
      implements IntegerColumnStatistics {

    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;
    private long sum = 0;
    private boolean hasMinimum = false;
    private boolean overflow = false;

    IntegerStatisticsImpl() {
    }

    IntegerStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.IntegerStatistics intStat = stats.getIntStatistics();
      if (intStat.hasMinimum()) {
        hasMinimum = true;
        minimum = intStat.getMinimum();
      }
      if (intStat.hasMaximum()) {
        maximum = intStat.getMaximum();
      }
      if (intStat.hasSum()) {
        sum = intStat.getSum();
      } else {
        overflow = true;
      }
    }

    @Override
    void reset() {
      super.reset();
      hasMinimum = false;
      minimum = Long.MAX_VALUE;
      maximum = Long.MIN_VALUE;
      sum = 0;
      overflow = false;
    }

    @Override
    void updateInteger(long value) {
      if (!hasMinimum) {
        hasMinimum = true;
        minimum = value;
        maximum = value;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      if (!overflow) {
        boolean wasPositive = sum >= 0;
        sum += value;
        if ((value >= 0) == wasPositive) {
          overflow = (sum >= 0) != wasPositive;
        }
      }
    }

    @Override
    void merge(ColumnStatisticsImpl other) {
      IntegerStatisticsImpl otherInt = (IntegerStatisticsImpl) other;
      if (!hasMinimum) {
        hasMinimum = otherInt.hasMinimum;
        minimum = otherInt.minimum;
        maximum = otherInt.maximum;
      } else if (otherInt.hasMinimum) {
        if (otherInt.minimum < minimum) {
          minimum = otherInt.minimum;
        }
        if (otherInt.maximum > maximum) {
          maximum = otherInt.maximum;
        }
      }
      super.merge(other);
      overflow |= otherInt.overflow;
      if (!overflow) {
        boolean wasPositive = sum >= 0;
        sum += otherInt.sum;
        if ((otherInt.sum >= 0) == wasPositive) {
          overflow = (sum >= 0) != wasPositive;
        }
      }
    }

    @Override
    OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.IntegerStatistics.Builder intb =
        OrcProto.IntegerStatistics.newBuilder();
      if (hasMinimum) {
        intb.setMinimum(minimum);
        intb.setMaximum(maximum);
      }
      if (!overflow) {
        intb.setSum(sum);
      }
      builder.setIntStatistics(intb);
      return builder;
    }

    @Override
    public long getMinimum() {
      return minimum;
    }

    @Override
    public long getMaximum() {
      return maximum;
    }

    @Override
    public boolean isSumDefined() {
      return !overflow;
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (hasMinimum) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      if (!overflow) {
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }
  }

  private static final class DoubleStatisticsImpl extends ColumnStatisticsImpl
       implements DoubleColumnStatistics {
    private boolean hasMinimum = false;
    private double minimum = Double.MAX_VALUE;
    private double maximum = Double.MIN_VALUE;
    private double sum = 0;

    DoubleStatisticsImpl() {
    }

    DoubleStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DoubleStatistics dbl = stats.getDoubleStatistics();
      if (dbl.hasMinimum()) {
        hasMinimum = true;
        minimum = dbl.getMinimum();
      }
      if (dbl.hasMaximum()) {
        maximum = dbl.getMaximum();
      }
      if (dbl.hasSum()) {
        sum = dbl.getSum();
      }
    }

    @Override
    void reset() {
      super.reset();
      hasMinimum = false;
      minimum = Double.MAX_VALUE;
      maximum = Double.MIN_VALUE;
      sum = 0;
    }

    @Override
    void updateDouble(double value) {
      if (!hasMinimum) {
        hasMinimum = true;
        minimum = value;
        maximum = value;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      sum += value;
    }

    @Override
    void merge(ColumnStatisticsImpl other) {
      super.merge(other);
      DoubleStatisticsImpl dbl = (DoubleStatisticsImpl) other;
      if (!hasMinimum) {
        hasMinimum = dbl.hasMinimum;
        minimum = dbl.minimum;
        maximum = dbl.maximum;
      } else if (dbl.hasMinimum) {
        if (dbl.minimum < minimum) {
          minimum = dbl.minimum;
        }
        if (dbl.maximum > maximum) {
          maximum = dbl.maximum;
        }
      }
      sum += dbl.sum;
    }

    @Override
    OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.DoubleStatistics.Builder dbl =
        OrcProto.DoubleStatistics.newBuilder();
      if (hasMinimum) {
        dbl.setMinimum(minimum);
        dbl.setMaximum(maximum);
      }
      dbl.setSum(sum);
      builder.setDoubleStatistics(dbl);
      return builder;
    }

    @Override
    public double getMinimum() {
      return minimum;
    }

    @Override
    public double getMaximum() {
      return maximum;
    }

    @Override
    public double getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (hasMinimum) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      buf.append(" sum: ");
      buf.append(sum);
      return buf.toString();
    }
  }

  protected static final class StringStatisticsImpl extends ColumnStatisticsImpl
      implements StringColumnStatistics {
    private String minimum = null;
    private String maximum = null;
    private long sum = 0;

    StringStatisticsImpl() {
    }

    StringStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.StringStatistics str = stats.getStringStatistics();
      if (str.hasMaximum()) {
        maximum = str.getMaximum();
      }
      if (str.hasMinimum()) {
        minimum = str.getMinimum();
      }
      if(str.hasSum()) {
        sum = str.getSum();
      }
    }

    @Override
    void reset() {
      super.reset();
      minimum = null;
      maximum = null;
      sum = 0;
    }

    @Override
    void updateString(String value) {
      if (minimum == null) {
        minimum = value;
        maximum = value;
      } else if (minimum.compareTo(value) > 0) {
        minimum = value;
      } else if (maximum.compareTo(value) < 0) {
        maximum = value;
      }
      sum += value.length();
    }

    @Override
    void merge(ColumnStatisticsImpl other) {
      super.merge(other);
      StringStatisticsImpl str = (StringStatisticsImpl) other;
      if (minimum == null) {
        minimum = str.minimum;
        maximum = str.maximum;
      } else if (str.minimum != null) {
        if (minimum.compareTo(str.minimum) > 0) {
          minimum = str.minimum;
        } else if (maximum.compareTo(str.maximum) < 0) {
          maximum = str.maximum;
        }
      }
      sum += str.sum;
    }

    @Override
    OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.StringStatistics.Builder str =
        OrcProto.StringStatistics.newBuilder();
      if (getNumberOfValues() != 0) {
        str.setMinimum(minimum);
        str.setMaximum(maximum);
        str.setSum(sum);
      }
      result.setStringStatistics(str);
      return result;
    }

    @Override
    public String getMinimum() {
      return minimum;
    }

    @Override
    public String getMaximum() {
      return maximum;
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }
  }

  protected static final class BinaryStatisticsImpl extends ColumnStatisticsImpl implements
      BinaryColumnStatistics {

    private long sum = 0;

    BinaryStatisticsImpl() {
    }

    BinaryStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.BinaryStatistics binStats = stats.getBinaryStatistics();
      if (binStats.hasSum()) {
        sum = binStats.getSum();
      }
    }

    @Override
    void reset() {
      super.reset();
      sum = 0;
    }

    @Override
    void updateBinary(BytesWritable value) {
      sum += value.getLength();
    }

    @Override
    void merge(ColumnStatisticsImpl other) {
      super.merge(other);
      BinaryStatisticsImpl bin = (BinaryStatisticsImpl) other;
      sum += bin.sum;
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.BinaryStatistics.Builder bin = OrcProto.BinaryStatistics.newBuilder();
      bin.setSum(sum);
      result.setBinaryStatistics(bin);
      return result;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }
  }

  private static final class DecimalStatisticsImpl extends ColumnStatisticsImpl
      implements DecimalColumnStatistics {
    private HiveDecimal minimum = null;
    private HiveDecimal maximum = null;
    private HiveDecimal sum = HiveDecimal.ZERO;

    DecimalStatisticsImpl() {
    }

    DecimalStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DecimalStatistics dec = stats.getDecimalStatistics();
      if (dec.hasMaximum()) {
        maximum = HiveDecimal.create(dec.getMaximum());
      }
      if (dec.hasMinimum()) {
        minimum = HiveDecimal.create(dec.getMinimum());
      }
      if (dec.hasSum()) {
        sum = HiveDecimal.create(dec.getSum());
      } else {
        sum = null;
      }
    }

    @Override
    void reset() {
      super.reset();
      minimum = null;
      maximum = null;
      sum = HiveDecimal.ZERO;
    }

    @Override
    void updateDecimal(HiveDecimal value) {
      if (minimum == null) {
        minimum = value;
        maximum = value;
      } else if (minimum.compareTo(value) > 0) {
        minimum = value;
      } else if (maximum.compareTo(value) < 0) {
        maximum = value;
      }
      if (sum != null) {
        sum = sum.add(value);
      }
    }

    @Override
    void merge(ColumnStatisticsImpl other) {
      super.merge(other);
      DecimalStatisticsImpl dec = (DecimalStatisticsImpl) other;
      if (minimum == null) {
        minimum = dec.minimum;
        maximum = dec.maximum;
        sum = dec.sum;
      } else if (dec.minimum != null) {
        if (minimum.compareTo(dec.minimum) > 0) {
          minimum = dec.minimum;
        } else if (maximum.compareTo(dec.maximum) < 0) {
          maximum = dec.maximum;
        }
        if (sum == null || dec.sum == null) {
          sum = null;
        } else {
          sum = sum.add(dec.sum);
        }
      }
    }

    @Override
    OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.DecimalStatistics.Builder dec =
          OrcProto.DecimalStatistics.newBuilder();
      if (getNumberOfValues() != 0) {
        dec.setMinimum(minimum.toString());
        dec.setMaximum(maximum.toString());
      }
      if (sum != null) {
        dec.setSum(sum.toString());
      }
      result.setDecimalStatistics(dec);
      return result;
    }

    @Override
    public HiveDecimal getMinimum() {
      return minimum;
    }

    @Override
    public HiveDecimal getMaximum() {
      return maximum;
    }

    @Override
    public HiveDecimal getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
        if (sum != null) {
          buf.append(" sum: ");
          buf.append(sum);
        }
      }
      return buf.toString();
    }
  }

  private static final class DateStatisticsImpl extends ColumnStatisticsImpl
      implements DateColumnStatistics {
    private DateWritable minimum = null;
    private DateWritable maximum = null;

    DateStatisticsImpl() {
    }

    DateStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DateStatistics dateStats = stats.getDateStatistics();
      // min,max values serialized/deserialized as int (days since epoch)
      if (dateStats.hasMaximum()) {
        maximum = new DateWritable(dateStats.getMaximum());
      }
      if (dateStats.hasMinimum()) {
        minimum = new DateWritable(dateStats.getMinimum());
      }
    }

    @Override
    void reset() {
      super.reset();
      minimum = null;
      maximum = null;
    }

    @Override
    void updateDate(DateWritable value) {
      if (minimum == null) {
        minimum = value;
        maximum = value;
      } else if (minimum.compareTo(value) > 0) {
        minimum = value;
      } else if (maximum.compareTo(value) < 0) {
        maximum = value;
      }
    }

    @Override
    void merge(ColumnStatisticsImpl other) {
      super.merge(other);
      DateStatisticsImpl dateStats = (DateStatisticsImpl) other;
      if (minimum == null) {
        minimum = dateStats.minimum;
        maximum = dateStats.maximum;
      } else if (dateStats.minimum != null) {
        if (minimum.compareTo(dateStats.minimum) > 0) {
          minimum = dateStats.minimum;
        } else if (maximum.compareTo(dateStats.maximum) < 0) {
          maximum = dateStats.maximum;
        }
      }
    }

    @Override
    OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.DateStatistics.Builder dateStats =
          OrcProto.DateStatistics.newBuilder();
      if (getNumberOfValues() != 0) {
        dateStats.setMinimum(minimum.getDays());
        dateStats.setMaximum(maximum.getDays());
      }
      result.setDateStatistics(dateStats);
      return result;
    }

    @Override
    public DateWritable getMinimum() {
      return minimum;
    }

    @Override
    public DateWritable getMaximum() {
      return maximum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      return buf.toString();
    }
  }

  private long count = 0;

  ColumnStatisticsImpl(OrcProto.ColumnStatistics stats) {
    if (stats.hasNumberOfValues()) {
      count = stats.getNumberOfValues();
    }
  }

  ColumnStatisticsImpl() {
  }

  void increment() {
    count += 1;
  }

  void updateBoolean(boolean value) {
    throw new UnsupportedOperationException("Can't update boolean");
  }

  void updateInteger(long value) {
    throw new UnsupportedOperationException("Can't update integer");
  }

  void updateDouble(double value) {
    throw new UnsupportedOperationException("Can't update double");
  }

  void updateString(String value) {
    throw new UnsupportedOperationException("Can't update string");
  }

  void updateBinary(BytesWritable value) {
    throw new UnsupportedOperationException("Can't update binary");
  }

  void updateDecimal(HiveDecimal value) {
    throw new UnsupportedOperationException("Can't update decimal");
  }

  void updateDate(DateWritable value) {
    throw new UnsupportedOperationException("Can't update date");
  }

  void merge(ColumnStatisticsImpl stats) {
    count += stats.count;
  }

  void reset() {
    count = 0;
  }

  @Override
  public long getNumberOfValues() {
    return count;
  }

  @Override
  public String toString() {
    return "count: " + count;
  }

  OrcProto.ColumnStatistics.Builder serialize() {
    OrcProto.ColumnStatistics.Builder builder =
      OrcProto.ColumnStatistics.newBuilder();
    builder.setNumberOfValues(count);
    return builder;
  }

  static ColumnStatisticsImpl create(ObjectInspector inspector) {
    switch (inspector.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveObjectInspector) inspector).getPrimitiveCategory()) {
          case BOOLEAN:
            return new BooleanStatisticsImpl();
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
            return new IntegerStatisticsImpl();
          case FLOAT:
          case DOUBLE:
            return new DoubleStatisticsImpl();
          case STRING:
          case VARCHAR:
            return new StringStatisticsImpl();
          case DECIMAL:
            return new DecimalStatisticsImpl();
          case DATE:
            return new DateStatisticsImpl();
          case BINARY:
            return new BinaryStatisticsImpl();
          default:
            return new ColumnStatisticsImpl();
        }
      default:
        return new ColumnStatisticsImpl();
    }
  }

  static ColumnStatisticsImpl deserialize(OrcProto.ColumnStatistics stats) {
    if (stats.hasBucketStatistics()) {
      return new BooleanStatisticsImpl(stats);
    } else if (stats.hasIntStatistics()) {
      return new IntegerStatisticsImpl(stats);
    } else if (stats.hasDoubleStatistics()) {
      return new DoubleStatisticsImpl(stats);
    } else if (stats.hasStringStatistics()) {
      return new StringStatisticsImpl(stats);
    } else if (stats.hasDecimalStatistics()) {
      return new DecimalStatisticsImpl(stats);
    } else if (stats.hasDateStatistics()) {
      return new DateStatisticsImpl(stats);
    } else if(stats.hasBinaryStatistics()) {
      return new BinaryStatisticsImpl(stats);
    } else {
      return new ColumnStatisticsImpl(stats);
    }
  }
}
