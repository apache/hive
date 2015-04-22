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

import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampUtils;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Factory for creating ORC tree readers.
 */
public class TreeReaderFactory {

  protected abstract static class TreeReader {
    protected final int columnId;
    protected BitFieldReader present = null;
    protected boolean valuePresent = false;

    TreeReader(int columnId) throws IOException {
      this(columnId, null);
    }

    TreeReader(int columnId, InStream in) throws IOException {
      this.columnId = columnId;
      if (in == null) {
        present = null;
        valuePresent = true;
      } else {
        present = new BitFieldReader(in, 1);
      }
    }

    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    IntegerReader createIntegerReader(OrcProto.ColumnEncoding.Kind kind,
        InStream in,
        boolean signed, boolean skipCorrupt) throws IOException {
      switch (kind) {
        case DIRECT_V2:
        case DICTIONARY_V2:
          return new RunLengthIntegerReaderV2(in, signed, skipCorrupt);
        case DIRECT:
        case DICTIONARY:
          return new RunLengthIntegerReader(in, signed);
        default:
          throw new IllegalArgumentException("Unknown encoding " + kind);
      }
    }

    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      checkEncoding(stripeFooter.getColumnsList().get(columnId));
      InStream in = streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.PRESENT));
      if (in == null) {
        present = null;
        valuePresent = true;
      } else {
        present = new BitFieldReader(in, 1);
      }
    }

    /**
     * Seek to the given position.
     *
     * @param index the indexes loaded from the file
     * @throws IOException
     */
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        present.seek(index);
      }
    }

    protected long countNonNulls(long rows) throws IOException {
      if (present != null) {
        long result = 0;
        for (long c = 0; c < rows; ++c) {
          if (present.next() == 1) {
            result += 1;
          }
        }
        return result;
      } else {
        return rows;
      }
    }

    abstract void skipRows(long rows) throws IOException;

    Object next(Object previous) throws IOException {
      if (present != null) {
        valuePresent = present.next() == 1;
      }
      return previous;
    }

    /**
     * Populates the isNull vector array in the previousVector object based on
     * the present stream values. This function is called from all the child
     * readers, and they all set the values based on isNull field value.
     *
     * @param previousVector The columnVector object whose isNull value is populated
     * @param batchSize      Size of the column vector
     * @return next column vector
     * @throws IOException
     */
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      ColumnVector result = (ColumnVector) previousVector;
      if (present != null) {
        // Set noNulls and isNull vector of the ColumnVector based on
        // present stream
        result.noNulls = true;
        for (int i = 0; i < batchSize; i++) {
          result.isNull[i] = (present.next() != 1);
          if (result.noNulls && result.isNull[i]) {
            result.noNulls = false;
          }
        }
      } else {
        // There is not present stream, this means that all the values are
        // present.
        result.noNulls = true;
        for (int i = 0; i < batchSize; i++) {
          result.isNull[i] = false;
        }
      }
      return previousVector;
    }
  }

  protected static class BooleanTreeReader extends TreeReader {
    protected BitFieldReader reader = null;

    BooleanTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    BooleanTreeReader(int columnId, InStream present, InStream data) throws IOException {
      super(columnId, present);
      if (data != null) {
        reader = new BitFieldReader(data, 1);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      reader = new BitFieldReader(streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA)), 1);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      BooleanWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new BooleanWritable();
        } else {
          result = (BooleanWritable) previous;
        }
        result.set(reader.next() == 1);
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final LongColumnVector result;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }
  }

  protected static class ByteTreeReader extends TreeReader {
    protected RunLengthByteReader reader = null;

    ByteTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    ByteTreeReader(int columnId, InStream present, InStream data) throws IOException {
      super(columnId, present);
      this.reader = new RunLengthByteReader(data);
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      reader = new RunLengthByteReader(streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA)));
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      ByteWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ByteWritable();
        } else {
          result = (ByteWritable) previous;
        }
        result.set(reader.next());
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final LongColumnVector result;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  protected static class ShortTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    ShortTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null);
    }

    ShortTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding)
        throws IOException {
      super(columnId, present);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.reader = createIntegerReader(encoding.getKind(), data, true, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(name), true, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      ShortWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ShortWritable();
        } else {
          result = (ShortWritable) previous;
        }
        result.set((short) reader.next());
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final LongColumnVector result;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  protected static class IntTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    IntTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null);
    }

    IntTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding)
        throws IOException {
      super(columnId, present);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.reader = createIntegerReader(encoding.getKind(), data, true, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(name), true, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      IntWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new IntWritable();
        } else {
          result = (IntWritable) previous;
        }
        result.set((int) reader.next());
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final LongColumnVector result;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  protected static class LongTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    LongTreeReader(int columnId, boolean skipCorrupt) throws IOException {
      this(columnId, null, null, null, skipCorrupt);
    }

    LongTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding,
        boolean skipCorrupt)
        throws IOException {
      super(columnId, present);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.reader = createIntegerReader(encoding.getKind(), data, true, skipCorrupt);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(name), true, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      LongWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new LongWritable();
        } else {
          result = (LongWritable) previous;
        }
        result.set(reader.next());
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final LongColumnVector result;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  protected static class FloatTreeReader extends TreeReader {
    protected InStream stream;
    private final SerializationUtils utils;

    FloatTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    FloatTreeReader(int columnId, InStream present, InStream data) throws IOException {
      super(columnId, present);
      this.utils = new SerializationUtils();
      this.stream = data;
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      stream.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      FloatWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new FloatWritable();
        } else {
          result = (FloatWritable) previous;
        }
        result.set(utils.readFloat(stream));
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, final long batchSize) throws IOException {
      final DoubleColumnVector result;
      if (previousVector == null) {
        result = new DoubleColumnVector();
      } else {
        result = (DoubleColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      final boolean hasNulls = !result.noNulls;
      boolean allNulls = hasNulls;

      if (hasNulls) {
        // conditions to ensure bounds checks skips
        for (int i = 0; batchSize <= result.isNull.length && i < batchSize; i++) {
          allNulls = allNulls & result.isNull[i];
        }
        if (allNulls) {
          result.vector[0] = Double.NaN;
          result.isRepeating = true;
        } else {
          // some nulls
          result.isRepeating = false;
          // conditions to ensure bounds checks skips
          for (int i = 0; batchSize <= result.isNull.length
              && batchSize <= result.vector.length && i < batchSize; i++) {
            if (!result.isNull[i]) {
              result.vector[i] = utils.readFloat(stream);
            } else {
              // If the value is not present then set NaN
              result.vector[i] = Double.NaN;
            }
          }
        }
      } else {
        // no nulls & > 1 row (check repeating)
        boolean repeating = (batchSize > 1);
        final float f1 = utils.readFloat(stream);
        result.vector[0] = f1;
        // conditions to ensure bounds checks skips
        for (int i = 1; i < batchSize && batchSize <= result.vector.length; i++) {
          final float f2 = utils.readFloat(stream);
          repeating = repeating && (f1 == f2);
          result.vector[i] = f2;
        }
        result.isRepeating = repeating;
      }
      return result;
    }

    @Override
    protected void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      for (int i = 0; i < items; ++i) {
        utils.readFloat(stream);
      }
    }
  }

  protected static class DoubleTreeReader extends TreeReader {
    protected InStream stream;
    private final SerializationUtils utils;

    DoubleTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    DoubleTreeReader(int columnId, InStream present, InStream data) throws IOException {
      super(columnId, present);
      this.utils = new SerializationUtils();
      this.stream = data;
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name =
          new StreamName(columnId,
              OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      stream.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      DoubleWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new DoubleWritable();
        } else {
          result = (DoubleWritable) previous;
        }
        result.set(utils.readDouble(stream));
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, final long batchSize) throws IOException {
      final DoubleColumnVector result;
      if (previousVector == null) {
        result = new DoubleColumnVector();
      } else {
        result = (DoubleColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      final boolean hasNulls = !result.noNulls;
      boolean allNulls = hasNulls;

      if (hasNulls) {
        // conditions to ensure bounds checks skips
        for (int i = 0; i < batchSize && batchSize <= result.isNull.length; i++) {
          allNulls = allNulls & result.isNull[i];
        }
        if (allNulls) {
          result.vector[0] = Double.NaN;
          result.isRepeating = true;
        } else {
          // some nulls
          result.isRepeating = false;
          // conditions to ensure bounds checks skips
          for (int i = 0; batchSize <= result.isNull.length
              && batchSize <= result.vector.length && i < batchSize; i++) {
            if (!result.isNull[i]) {
              result.vector[i] = utils.readDouble(stream);
            } else {
              // If the value is not present then set NaN
              result.vector[i] = Double.NaN;
            }
          }
        }
      } else {
        // no nulls
        boolean repeating = (batchSize > 1);
        final double d1 = utils.readDouble(stream);
        result.vector[0] = d1;
        // conditions to ensure bounds checks skips
        for (int i = 1; i < batchSize && batchSize <= result.vector.length; i++) {
          final double d2 = utils.readDouble(stream);
          repeating = repeating && (d1 == d2);
          result.vector[i] = d2;
        }
        result.isRepeating = repeating;
      }

      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long len = items * 8;
      while (len > 0) {
        len -= stream.skip(len);
      }
    }
  }

  protected static class BinaryTreeReader extends TreeReader {
    protected InStream stream;
    protected IntegerReader lengths = null;
    protected final LongColumnVector scratchlcv;

    BinaryTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null, null);
    }

    BinaryTreeReader(int columnId, InStream present, InStream data, InStream length,
        OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present);
      scratchlcv = new LongColumnVector();
      this.stream = data;
      if (length != null && encoding != null) {
        checkEncoding(encoding);
        this.lengths = createIntegerReader(encoding.getKind(), length, false, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
      lengths = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(new StreamName(columnId, OrcProto.Stream.Kind.LENGTH)), false, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      stream.seek(index);
      lengths.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      BytesWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new BytesWritable();
        } else {
          result = (BytesWritable) previous;
        }
        int len = (int) lengths.next();
        result.setSize(len);
        int offset = 0;
        while (len > 0) {
          int written = stream.read(result.getBytes(), offset, len);
          if (written < 0) {
            throw new EOFException("Can't finish byte read from " + stream);
          }
          len -= written;
          offset += written;
        }
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final BytesColumnVector result;
      if (previousVector == null) {
        result = new BytesColumnVector();
      } else {
        result = (BytesColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      BytesColumnVectorUtil.readOrcByteArrays(stream, lengths, scratchlcv, result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long lengthToSkip = 0;
      for (int i = 0; i < items; ++i) {
        lengthToSkip += lengths.next();
      }
      while (lengthToSkip > 0) {
        lengthToSkip -= stream.skip(lengthToSkip);
      }
    }
  }

  protected static class TimestampTreeReader extends TreeReader {
    protected IntegerReader data = null;
    protected IntegerReader nanos = null;
    private final boolean skipCorrupt;
    private Map<String, Long> baseTimestampMap;
    private long base_timestamp;
    private final TimeZone readerTimeZone;
    private TimeZone writerTimeZone;
    private boolean hasSameTZRules;

    TimestampTreeReader(int columnId, boolean skipCorrupt) throws IOException {
      this(columnId, null, null, null, null, skipCorrupt);
    }

    TimestampTreeReader(int columnId, InStream presentStream, InStream dataStream,
        InStream nanosStream, OrcProto.ColumnEncoding encoding, boolean skipCorrupt)
        throws IOException {
      super(columnId, presentStream);
      this.skipCorrupt = skipCorrupt;
      this.baseTimestampMap = new HashMap<>();
      this.readerTimeZone = TimeZone.getDefault();
      this.writerTimeZone = readerTimeZone;
      this.hasSameTZRules = writerTimeZone.hasSameRules(readerTimeZone);
      this.base_timestamp = getBaseTimestamp(readerTimeZone.getID());
      if (encoding != null) {
        checkEncoding(encoding);

        if (dataStream != null) {
          this.data = createIntegerReader(encoding.getKind(), dataStream, true, skipCorrupt);
        }

        if (nanosStream != null) {
          this.nanos = createIntegerReader(encoding.getKind(), nanosStream, false, skipCorrupt);
        }
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      data = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.DATA)), true, skipCorrupt);
      nanos = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.SECONDARY)), false, skipCorrupt);
      base_timestamp = getBaseTimestamp(stripeFooter.getWriterTimezone());
    }

    private long getBaseTimestamp(String timeZoneId) throws IOException {
      // to make sure new readers read old files in the same way
      if (timeZoneId == null || timeZoneId.isEmpty()) {
        timeZoneId = readerTimeZone.getID();
      }

      if (!baseTimestampMap.containsKey(timeZoneId)) {
        writerTimeZone = TimeZone.getTimeZone(timeZoneId);
        hasSameTZRules = writerTimeZone.hasSameRules(readerTimeZone);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(writerTimeZone);
        try {
          long epoch =
              sdf.parse(WriterImpl.BASE_TIMESTAMP_STRING).getTime() / WriterImpl.MILLIS_PER_SECOND;
          baseTimestampMap.put(timeZoneId, epoch);
          return epoch;
        } catch (ParseException e) {
          throw new IOException("Unable to create base timestamp", e);
        } finally {
          sdf.setTimeZone(readerTimeZone);
        }
      }

      return baseTimestampMap.get(timeZoneId);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      data.seek(index);
      nanos.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      TimestampWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new TimestampWritable();
        } else {
          result = (TimestampWritable) previous;
        }
        long millis = (data.next() + base_timestamp) * WriterImpl.MILLIS_PER_SECOND;
        int newNanos = parseNanos(nanos.next());
        // fix the rounding when we divided by 1000.
        if (millis >= 0) {
          millis += newNanos / 1000000;
        } else {
          millis -= newNanos / 1000000;
        }
        long offset = 0;
        // If reader and writer time zones have different rules, adjust the timezone difference
        // between reader and writer taking day light savings into account.
        if (!hasSameTZRules) {
          offset = writerTimeZone.getOffset(millis) - readerTimeZone.getOffset(millis);
        }
        long adjustedMillis = millis + offset;
        Timestamp ts = new Timestamp(adjustedMillis);
        // Sometimes the reader timezone might have changed after adding the adjustedMillis.
        // To account for that change, check for any difference in reader timezone after
        // adding adjustedMillis. If so use the new offset (offset at adjustedMillis point of time).
        if (!hasSameTZRules &&
            (readerTimeZone.getOffset(millis) != readerTimeZone.getOffset(adjustedMillis))) {
          long newOffset =
              writerTimeZone.getOffset(millis) - readerTimeZone.getOffset(adjustedMillis);
          adjustedMillis = millis + newOffset;
          ts.setTime(adjustedMillis);
        }
        ts.setNanos(newNanos);
        result.set(ts);
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final LongColumnVector result;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      result.reset();
      Object obj = null;
      for (int i = 0; i < batchSize; i++) {
        obj = next(obj);
        if (obj == null) {
          result.noNulls = false;
          result.isNull[i] = true;
        } else {
          TimestampWritable writable = (TimestampWritable) obj;
          Timestamp timestamp = writable.getTimestamp();
          result.vector[i] = TimestampUtils.getTimeNanoSec(timestamp);
        }
      }

      return result;
    }

    private static int parseNanos(long serialized) {
      int zeros = 7 & (int) serialized;
      int result = (int) (serialized >>> 3);
      if (zeros != 0) {
        for (int i = 0; i <= zeros; ++i) {
          result *= 10;
        }
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      data.skip(items);
      nanos.skip(items);
    }
  }

  protected static class DateTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    DateTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null);
    }

    DateTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        reader = createIntegerReader(encoding.getKind(), data, true, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(name), true, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      DateWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new DateWritable();
        } else {
          result = (DateWritable) previous;
        }
        result.set((int) reader.next());
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final LongColumnVector result;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  protected static class DecimalTreeReader extends TreeReader {
    protected InStream valueStream;
    protected IntegerReader scaleReader = null;
    private LongColumnVector scratchScaleVector;

    private final int precision;
    private final int scale;

    DecimalTreeReader(int columnId, int precision, int scale) throws IOException {
      this(columnId, precision, scale, null, null, null, null);
    }

    DecimalTreeReader(int columnId, int precision, int scale, InStream present,
        InStream valueStream, InStream scaleStream, OrcProto.ColumnEncoding encoding)
        throws IOException {
      super(columnId, present);
      this.precision = precision;
      this.scale = scale;
      this.scratchScaleVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      this.valueStream = valueStream;
      if (scaleStream != null && encoding != null) {
        checkEncoding(encoding);
        this.scaleReader = createIntegerReader(encoding.getKind(), scaleStream, true, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      valueStream = streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA));
      scaleReader = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(new StreamName(columnId, OrcProto.Stream.Kind.SECONDARY)), true, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      valueStream.seek(index);
      scaleReader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      final HiveDecimalWritable result;
      if (valuePresent) {
        if (previous == null) {
          result = new HiveDecimalWritable();
        } else {
          result = (HiveDecimalWritable) previous;
        }
        result.set(HiveDecimal.create(SerializationUtils.readBigInteger(valueStream),
            (int) scaleReader.next()));
        return HiveDecimalUtils.enforcePrecisionScale(result, precision, scale);
      }
      return null;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final DecimalColumnVector result;
      if (previousVector == null) {
        result = new DecimalColumnVector(precision, scale);
      } else {
        result = (DecimalColumnVector) previousVector;
      }

      // Save the reference for isNull in the scratch vector
      boolean[] scratchIsNull = scratchScaleVector.isNull;

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      if (result.isRepeating) {
        if (!result.isNull[0]) {
          BigInteger bInt = SerializationUtils.readBigInteger(valueStream);
          short scaleInData = (short) scaleReader.next();
          HiveDecimal dec = HiveDecimal.create(bInt, scaleInData);
          dec = HiveDecimalUtils.enforcePrecisionScale(dec, precision, scale);
          result.set(0, dec);
        }
      } else {
        // result vector has isNull values set, use the same to read scale vector.
        scratchScaleVector.isNull = result.isNull;
        scaleReader.nextVector(scratchScaleVector, batchSize);
        for (int i = 0; i < batchSize; i++) {
          if (!result.isNull[i]) {
            BigInteger bInt = SerializationUtils.readBigInteger(valueStream);
            short scaleInData = (short) scratchScaleVector.vector[i];
            HiveDecimal dec = HiveDecimal.create(bInt, scaleInData);
            dec = HiveDecimalUtils.enforcePrecisionScale(dec, precision, scale);
            result.set(i, dec);
          }
        }
      }
      // Switch back the null vector.
      scratchScaleVector.isNull = scratchIsNull;
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      for (int i = 0; i < items; i++) {
        SerializationUtils.readBigInteger(valueStream);
      }
      scaleReader.skip(items);
    }
  }

  /**
   * A tree reader that will read string columns. At the start of the
   * stripe, it creates an internal reader based on whether a direct or
   * dictionary encoding was used.
   */
  protected static class StringTreeReader extends TreeReader {
    protected TreeReader reader;

    StringTreeReader(int columnId) throws IOException {
      super(columnId);
    }

    StringTreeReader(int columnId, InStream present, InStream data, InStream length,
        InStream dictionary, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present);
      if (encoding != null) {
        switch (encoding.getKind()) {
          case DIRECT:
          case DIRECT_V2:
            reader = new StringDirectTreeReader(columnId, present, data, length,
                encoding.getKind());
            break;
          case DICTIONARY:
          case DICTIONARY_V2:
            reader = new StringDictionaryTreeReader(columnId, present, data, length, dictionary,
                encoding);
            break;
          default:
            throw new IllegalArgumentException("Unsupported encoding " +
                encoding.getKind());
        }
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      reader.checkEncoding(encoding);
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      // For each stripe, checks the encoding and initializes the appropriate
      // reader
      switch (stripeFooter.getColumnsList().get(columnId).getKind()) {
        case DIRECT:
        case DIRECT_V2:
          reader = new StringDirectTreeReader(columnId);
          break;
        case DICTIONARY:
        case DICTIONARY_V2:
          reader = new StringDictionaryTreeReader(columnId);
          break;
        default:
          throw new IllegalArgumentException("Unsupported encoding " +
              stripeFooter.getColumnsList().get(columnId).getKind());
      }
      reader.startStripe(streams, stripeFooter);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      reader.seek(index);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      return reader.next(previous);
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      return reader.nextVector(previousVector, batchSize);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skipRows(items);
    }
  }

  // This class collects together very similar methods for reading an ORC vector of byte arrays and
  // creating the BytesColumnVector.
  //
  public static class BytesColumnVectorUtil {

    private static byte[] commonReadByteArrays(InStream stream, IntegerReader lengths,
        LongColumnVector scratchlcv,
        BytesColumnVector result, long batchSize) throws IOException {
      // Read lengths
      scratchlcv.isNull = result.isNull;  // Notice we are replacing the isNull vector here...
      lengths.nextVector(scratchlcv, batchSize);
      int totalLength = 0;
      if (!scratchlcv.isRepeating) {
        for (int i = 0; i < batchSize; i++) {
          if (!scratchlcv.isNull[i]) {
            totalLength += (int) scratchlcv.vector[i];
          }
        }
      } else {
        if (!scratchlcv.isNull[0]) {
          totalLength = (int) (batchSize * scratchlcv.vector[0]);
        }
      }

      // Read all the strings for this batch
      byte[] allBytes = new byte[totalLength];
      int offset = 0;
      int len = totalLength;
      while (len > 0) {
        int bytesRead = stream.read(allBytes, offset, len);
        if (bytesRead < 0) {
          throw new EOFException("Can't finish byte read from " + stream);
        }
        len -= bytesRead;
        offset += bytesRead;
      }

      return allBytes;
    }

    // This method has the common code for reading in bytes into a BytesColumnVector.
    public static void readOrcByteArrays(InStream stream, IntegerReader lengths,
        LongColumnVector scratchlcv,
        BytesColumnVector result, long batchSize) throws IOException {

      byte[] allBytes = commonReadByteArrays(stream, lengths, scratchlcv, result, batchSize);

      // Too expensive to figure out 'repeating' by comparisons.
      result.isRepeating = false;
      int offset = 0;
      if (!scratchlcv.isRepeating) {
        for (int i = 0; i < batchSize; i++) {
          if (!scratchlcv.isNull[i]) {
            result.setRef(i, allBytes, offset, (int) scratchlcv.vector[i]);
            offset += scratchlcv.vector[i];
          } else {
            result.setRef(i, allBytes, 0, 0);
          }
        }
      } else {
        for (int i = 0; i < batchSize; i++) {
          if (!scratchlcv.isNull[i]) {
            result.setRef(i, allBytes, offset, (int) scratchlcv.vector[0]);
            offset += scratchlcv.vector[0];
          } else {
            result.setRef(i, allBytes, 0, 0);
          }
        }
      }
    }
  }

  /**
   * A reader for string columns that are direct encoded in the current
   * stripe.
   */
  protected static class StringDirectTreeReader extends TreeReader {
    protected InStream stream;
    protected IntegerReader lengths;
    private final LongColumnVector scratchlcv;

    StringDirectTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null, null);
    }

    StringDirectTreeReader(int columnId, InStream present, InStream data, InStream length,
        OrcProto.ColumnEncoding.Kind encoding) throws IOException {
      super(columnId, present);
      this.scratchlcv = new LongColumnVector();
      this.stream = data;
      if (length != null && encoding != null) {
        this.lengths = createIntegerReader(encoding, length, false, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT &&
          encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
      lengths = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(new StreamName(columnId, OrcProto.Stream.Kind.LENGTH)),
          false, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      stream.seek(index);
      lengths.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      Text result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new Text();
        } else {
          result = (Text) previous;
        }
        int len = (int) lengths.next();
        int offset = 0;
        byte[] bytes = new byte[len];
        while (len > 0) {
          int written = stream.read(bytes, offset, len);
          if (written < 0) {
            throw new EOFException("Can't finish byte read from " + stream);
          }
          len -= written;
          offset += written;
        }
        result.set(bytes);
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final BytesColumnVector result;
      if (previousVector == null) {
        result = new BytesColumnVector();
      } else {
        result = (BytesColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      BytesColumnVectorUtil.readOrcByteArrays(stream, lengths, scratchlcv, result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long lengthToSkip = 0;
      for (int i = 0; i < items; ++i) {
        lengthToSkip += lengths.next();
      }

      while (lengthToSkip > 0) {
        lengthToSkip -= stream.skip(lengthToSkip);
      }
    }
  }

  /**
   * A reader for string columns that are dictionary encoded in the current
   * stripe.
   */
  protected static class StringDictionaryTreeReader extends TreeReader {
    private DynamicByteArray dictionaryBuffer;
    private int[] dictionaryOffsets;
    protected IntegerReader reader;

    private byte[] dictionaryBufferInBytesCache = null;
    private final LongColumnVector scratchlcv;

    StringDictionaryTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null, null, null);
    }

    StringDictionaryTreeReader(int columnId, InStream present, InStream data,
        InStream length, InStream dictionary, OrcProto.ColumnEncoding encoding)
        throws IOException {
      super(columnId, present);
      scratchlcv = new LongColumnVector();
      if (data != null && encoding != null) {
        this.reader = createIntegerReader(encoding.getKind(), data, false, false);
      }

      if (dictionary != null && encoding != null) {
        readDictionaryStream(dictionary);
      }

      if (length != null && encoding != null) {
        readDictionaryLengthStream(length, encoding);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DICTIONARY &&
          encoding.getKind() != OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);

      // read the dictionary blob
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DICTIONARY_DATA);
      InStream in = streams.get(name);
      readDictionaryStream(in);

      // read the lengths
      name = new StreamName(columnId, OrcProto.Stream.Kind.LENGTH);
      in = streams.get(name);
      readDictionaryLengthStream(in, stripeFooter.getColumnsList().get(columnId));

      // set up the row reader
      name = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(name), false, false);
    }

    private void readDictionaryLengthStream(InStream in, OrcProto.ColumnEncoding encoding)
        throws IOException {
      int dictionarySize = encoding.getDictionarySize();
      if (in != null) { // Guard against empty LENGTH stream.
        IntegerReader lenReader = createIntegerReader(encoding.getKind(), in, false, false);
        int offset = 0;
        if (dictionaryOffsets == null ||
            dictionaryOffsets.length < dictionarySize + 1) {
          dictionaryOffsets = new int[dictionarySize + 1];
        }
        for (int i = 0; i < dictionarySize; ++i) {
          dictionaryOffsets[i] = offset;
          offset += (int) lenReader.next();
        }
        dictionaryOffsets[dictionarySize] = offset;
        in.close();
      }

    }

    private void readDictionaryStream(InStream in) throws IOException {
      if (in != null) { // Guard against empty dictionary stream.
        if (in.available() > 0) {
          dictionaryBuffer = new DynamicByteArray(64, in.available());
          dictionaryBuffer.readAll(in);
          // Since its start of strip invalidate the cache.
          dictionaryBufferInBytesCache = null;
        }
        in.close();
      } else {
        dictionaryBuffer = null;
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      Text result = null;
      if (valuePresent) {
        int entry = (int) reader.next();
        if (previous == null) {
          result = new Text();
        } else {
          result = (Text) previous;
        }
        int offset = dictionaryOffsets[entry];
        int length = getDictionaryEntryLength(entry, offset);
        // If the column is just empty strings, the size will be zero,
        // so the buffer will be null, in that case just return result
        // as it will default to empty
        if (dictionaryBuffer != null) {
          dictionaryBuffer.setText(result, offset, length);
        } else {
          result.clear();
        }
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final BytesColumnVector result;
      int offset;
      int length;
      if (previousVector == null) {
        result = new BytesColumnVector();
      } else {
        result = (BytesColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      if (dictionaryBuffer != null) {

        // Load dictionaryBuffer into cache.
        if (dictionaryBufferInBytesCache == null) {
          dictionaryBufferInBytesCache = dictionaryBuffer.get();
        }

        // Read string offsets
        scratchlcv.isNull = result.isNull;
        reader.nextVector(scratchlcv, batchSize);
        if (!scratchlcv.isRepeating) {

          // The vector has non-repeating strings. Iterate thru the batch
          // and set strings one by one
          for (int i = 0; i < batchSize; i++) {
            if (!scratchlcv.isNull[i]) {
              offset = dictionaryOffsets[(int) scratchlcv.vector[i]];
              length = getDictionaryEntryLength((int) scratchlcv.vector[i], offset);
              result.setRef(i, dictionaryBufferInBytesCache, offset, length);
            } else {
              // If the value is null then set offset and length to zero (null string)
              result.setRef(i, dictionaryBufferInBytesCache, 0, 0);
            }
          }
        } else {
          // If the value is repeating then just set the first value in the
          // vector and set the isRepeating flag to true. No need to iterate thru and
          // set all the elements to the same value
          offset = dictionaryOffsets[(int) scratchlcv.vector[0]];
          length = getDictionaryEntryLength((int) scratchlcv.vector[0], offset);
          result.setRef(0, dictionaryBufferInBytesCache, offset, length);
        }
        result.isRepeating = scratchlcv.isRepeating;
      } else {
        // Entire stripe contains null strings.
        result.isRepeating = true;
        result.noNulls = false;
        result.isNull[0] = true;
        result.setRef(0, "".getBytes(), 0, 0);
      }
      return result;
    }

    int getDictionaryEntryLength(int entry, int offset) {
      final int length;
      // if it isn't the last entry, subtract the offsets otherwise use
      // the buffer length.
      if (entry < dictionaryOffsets.length - 1) {
        length = dictionaryOffsets[entry + 1] - offset;
      } else {
        length = dictionaryBuffer.size() - offset;
      }
      return length;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  protected static class CharTreeReader extends StringTreeReader {
    int maxLength;

    CharTreeReader(int columnId, int maxLength) throws IOException {
      this(columnId, maxLength, null, null, null, null, null);
    }

    CharTreeReader(int columnId, int maxLength, InStream present, InStream data,
        InStream length, InStream dictionary, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present, data, length, dictionary, encoding);
      this.maxLength = maxLength;
    }

    @Override
    Object next(Object previous) throws IOException {
      final HiveCharWritable result;
      if (previous == null) {
        result = new HiveCharWritable();
      } else {
        result = (HiveCharWritable) previous;
      }
      // Use the string reader implementation to populate the internal Text value
      Object textVal = super.next(result.getTextValue());
      if (textVal == null) {
        return null;
      }
      // result should now hold the value that was read in.
      // enforce char length
      result.enforceMaxLength(maxLength);
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      // Get the vector of strings from StringTreeReader, then make a 2nd pass to
      // adjust down the length (right trim and truncate) if necessary.
      BytesColumnVector result = (BytesColumnVector) super.nextVector(previousVector, batchSize);

      int adjustedDownLen;
      if (result.isRepeating) {
        if (result.noNulls || !result.isNull[0]) {
          adjustedDownLen = StringExpr
              .rightTrimAndTruncate(result.vector[0], result.start[0], result.length[0], maxLength);
          if (adjustedDownLen < result.length[0]) {
            result.setRef(0, result.vector[0], result.start[0], adjustedDownLen);
          }
        }
      } else {
        if (result.noNulls) {
          for (int i = 0; i < batchSize; i++) {
            adjustedDownLen = StringExpr
                .rightTrimAndTruncate(result.vector[i], result.start[i], result.length[i],
                    maxLength);
            if (adjustedDownLen < result.length[i]) {
              result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
            }
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            if (!result.isNull[i]) {
              adjustedDownLen = StringExpr
                  .rightTrimAndTruncate(result.vector[i], result.start[i], result.length[i],
                      maxLength);
              if (adjustedDownLen < result.length[i]) {
                result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
              }
            }
          }
        }
      }
      return result;
    }
  }

  protected static class VarcharTreeReader extends StringTreeReader {
    int maxLength;

    VarcharTreeReader(int columnId, int maxLength) throws IOException {
      this(columnId, maxLength, null, null, null, null, null);
    }

    VarcharTreeReader(int columnId, int maxLength, InStream present, InStream data,
        InStream length, InStream dictionary, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present, data, length, dictionary, encoding);
      this.maxLength = maxLength;
    }

    @Override
    Object next(Object previous) throws IOException {
      final HiveVarcharWritable result;
      if (previous == null) {
        result = new HiveVarcharWritable();
      } else {
        result = (HiveVarcharWritable) previous;
      }
      // Use the string reader implementation to populate the internal Text value
      Object textVal = super.next(result.getTextValue());
      if (textVal == null) {
        return null;
      }
      // result should now hold the value that was read in.
      // enforce varchar length
      result.enforceMaxLength(maxLength);
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      // Get the vector of strings from StringTreeReader, then make a 2nd pass to
      // adjust down the length (truncate) if necessary.
      BytesColumnVector result = (BytesColumnVector) super.nextVector(previousVector, batchSize);

      int adjustedDownLen;
      if (result.isRepeating) {
        if (result.noNulls || !result.isNull[0]) {
          adjustedDownLen = StringExpr
              .truncate(result.vector[0], result.start[0], result.length[0], maxLength);
          if (adjustedDownLen < result.length[0]) {
            result.setRef(0, result.vector[0], result.start[0], adjustedDownLen);
          }
        }
      } else {
        if (result.noNulls) {
          for (int i = 0; i < batchSize; i++) {
            adjustedDownLen = StringExpr
                .truncate(result.vector[i], result.start[i], result.length[i], maxLength);
            if (adjustedDownLen < result.length[i]) {
              result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
            }
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            if (!result.isNull[i]) {
              adjustedDownLen = StringExpr
                  .truncate(result.vector[i], result.start[i], result.length[i], maxLength);
              if (adjustedDownLen < result.length[i]) {
                result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
              }
            }
          }
        }
      }
      return result;
    }
  }

  protected static class StructTreeReader extends TreeReader {
    protected final TreeReader[] fields;
    private final String[] fieldNames;

    StructTreeReader(int columnId,
        List<OrcProto.Type> types,
        boolean[] included,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getFieldNamesCount();
      this.fields = new TreeReader[fieldCount];
      this.fieldNames = new String[fieldCount];
      for (int i = 0; i < fieldCount; ++i) {
        int subtype = type.getSubtypes(i);
        if (included == null || included[subtype]) {
          this.fields[i] = createTreeReader(subtype, types, included, skipCorrupt);
        }
        this.fieldNames[i] = type.getFieldNames(i);
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      for (TreeReader kid : fields) {
        if (kid != null) {
          kid.seek(index);
        }
      }
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      OrcStruct result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new OrcStruct(fields.length);
        } else {
          result = (OrcStruct) previous;

          // If the input format was initialized with a file with a
          // different number of fields, the number of fields needs to
          // be updated to the correct number
          if (result.getNumFields() != fields.length) {
            result.setNumFields(fields.length);
          }
        }
        for (int i = 0; i < fields.length; ++i) {
          if (fields[i] != null) {
            result.setFieldValue(i, fields[i].next(result.getFieldValue(i)));
          }
        }
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final ColumnVector[] result;
      if (previousVector == null) {
        result = new ColumnVector[fields.length];
      } else {
        result = (ColumnVector[]) previousVector;
      }

      // Read all the members of struct as column vectors
      for (int i = 0; i < fields.length; i++) {
        if (fields[i] != null) {
          if (result[i] == null) {
            result[i] = (ColumnVector) fields[i].nextVector(null, batchSize);
          } else {
            fields[i].nextVector(result[i], batchSize);
          }
        }
      }
      return result;
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      for (TreeReader field : fields) {
        if (field != null) {
          field.startStripe(streams, stripeFooter);
        }
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      for (TreeReader field : fields) {
        if (field != null) {
          field.skipRows(items);
        }
      }
    }
  }

  protected static class UnionTreeReader extends TreeReader {
    protected final TreeReader[] fields;
    protected RunLengthByteReader tags;

    UnionTreeReader(int columnId,
        List<OrcProto.Type> types,
        boolean[] included,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getSubtypesCount();
      this.fields = new TreeReader[fieldCount];
      for (int i = 0; i < fieldCount; ++i) {
        int subtype = type.getSubtypes(i);
        if (included == null || included[subtype]) {
          this.fields[i] = createTreeReader(subtype, types, included, skipCorrupt);
        }
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      tags.seek(index[columnId]);
      for (TreeReader kid : fields) {
        kid.seek(index);
      }
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      OrcUnion result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new OrcUnion();
        } else {
          result = (OrcUnion) previous;
        }
        byte tag = tags.next();
        Object previousVal = result.getObject();
        result.set(tag, fields[tag].next(tag == result.getTag() ?
            previousVal : null));
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      throw new UnsupportedOperationException(
          "NextVector is not supported operation for Union type");
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      tags = new RunLengthByteReader(streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA)));
      for (TreeReader field : fields) {
        if (field != null) {
          field.startStripe(streams, stripeFooter);
        }
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long[] counts = new long[fields.length];
      for (int i = 0; i < items; ++i) {
        counts[tags.next()] += 1;
      }
      for (int i = 0; i < counts.length; ++i) {
        fields[i].skipRows(counts[i]);
      }
    }
  }

  protected static class ListTreeReader extends TreeReader {
    protected final TreeReader elementReader;
    protected IntegerReader lengths = null;

    ListTreeReader(int columnId,
        List<OrcProto.Type> types,
        boolean[] included,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      elementReader = createTreeReader(type.getSubtypes(0), types, included, skipCorrupt);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      lengths.seek(index[columnId]);
      elementReader.seek(index);
    }

    @Override
    @SuppressWarnings("unchecked")
    Object next(Object previous) throws IOException {
      super.next(previous);
      List<Object> result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ArrayList<>();
        } else {
          result = (ArrayList<Object>) previous;
        }
        int prevLength = result.size();
        int length = (int) lengths.next();
        // extend the list to the new length
        for (int i = prevLength; i < length; ++i) {
          result.add(null);
        }
        // read the new elements into the array
        for (int i = 0; i < length; i++) {
          result.set(i, elementReader.next(i < prevLength ?
              result.get(i) : null));
        }
        // remove any extra elements
        for (int i = prevLength - 1; i >= length; --i) {
          result.remove(i);
        }
      }
      return result;
    }

    @Override
    public Object nextVector(Object previous, long batchSize) throws IOException {
      throw new UnsupportedOperationException(
          "NextVector is not supported operation for List type");
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      lengths = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.LENGTH)), false, false);
      if (elementReader != null) {
        elementReader.startStripe(streams, stripeFooter);
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long childSkip = 0;
      for (long i = 0; i < items; ++i) {
        childSkip += lengths.next();
      }
      elementReader.skipRows(childSkip);
    }
  }

  protected static class MapTreeReader extends TreeReader {
    protected final TreeReader keyReader;
    protected final TreeReader valueReader;
    protected IntegerReader lengths = null;

    MapTreeReader(int columnId,
        List<OrcProto.Type> types,
        boolean[] included,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      int keyColumn = type.getSubtypes(0);
      int valueColumn = type.getSubtypes(1);
      if (included == null || included[keyColumn]) {
        keyReader = createTreeReader(keyColumn, types, included, skipCorrupt);
      } else {
        keyReader = null;
      }
      if (included == null || included[valueColumn]) {
        valueReader = createTreeReader(valueColumn, types, included, skipCorrupt);
      } else {
        valueReader = null;
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      lengths.seek(index[columnId]);
      keyReader.seek(index);
      valueReader.seek(index);
    }

    @Override
    @SuppressWarnings("unchecked")
    Object next(Object previous) throws IOException {
      super.next(previous);
      Map<Object, Object> result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new LinkedHashMap<>();
        } else {
          result = (LinkedHashMap<Object, Object>) previous;
        }
        // for now just clear and create new objects
        result.clear();
        int length = (int) lengths.next();
        // read the new elements into the array
        for (int i = 0; i < length; i++) {
          result.put(keyReader.next(null), valueReader.next(null));
        }
      }
      return result;
    }

    @Override
    public Object nextVector(Object previous, long batchSize) throws IOException {
      throw new UnsupportedOperationException(
          "NextVector is not supported operation for Map type");
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      lengths = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.LENGTH)), false, false);
      if (keyReader != null) {
        keyReader.startStripe(streams, stripeFooter);
      }
      if (valueReader != null) {
        valueReader.startStripe(streams, stripeFooter);
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long childSkip = 0;
      for (long i = 0; i < items; ++i) {
        childSkip += lengths.next();
      }
      keyReader.skipRows(childSkip);
      valueReader.skipRows(childSkip);
    }
  }

  public static TreeReader createTreeReader(int columnId,
      List<OrcProto.Type> types,
      boolean[] included,
      boolean skipCorrupt
  ) throws IOException {
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case BOOLEAN:
        return new BooleanTreeReader(columnId);
      case BYTE:
        return new ByteTreeReader(columnId);
      case DOUBLE:
        return new DoubleTreeReader(columnId);
      case FLOAT:
        return new FloatTreeReader(columnId);
      case SHORT:
        return new ShortTreeReader(columnId);
      case INT:
        return new IntTreeReader(columnId);
      case LONG:
        return new LongTreeReader(columnId, skipCorrupt);
      case STRING:
        return new StringTreeReader(columnId);
      case CHAR:
        if (!type.hasMaximumLength()) {
          throw new IllegalArgumentException("ORC char type has no length specified");
        }
        return new CharTreeReader(columnId, type.getMaximumLength());
      case VARCHAR:
        if (!type.hasMaximumLength()) {
          throw new IllegalArgumentException("ORC varchar type has no length specified");
        }
        return new VarcharTreeReader(columnId, type.getMaximumLength());
      case BINARY:
        return new BinaryTreeReader(columnId);
      case TIMESTAMP:
        return new TimestampTreeReader(columnId, skipCorrupt);
      case DATE:
        return new DateTreeReader(columnId);
      case DECIMAL:
        int precision =
            type.hasPrecision() ? type.getPrecision() : HiveDecimal.SYSTEM_DEFAULT_PRECISION;
        int scale = type.hasScale() ? type.getScale() : HiveDecimal.SYSTEM_DEFAULT_SCALE;
        return new DecimalTreeReader(columnId, precision, scale);
      case STRUCT:
        return new StructTreeReader(columnId, types, included, skipCorrupt);
      case LIST:
        return new ListTreeReader(columnId, types, included, skipCorrupt);
      case MAP:
        return new MapTreeReader(columnId, types, included, skipCorrupt);
      case UNION:
        return new UnionTreeReader(columnId, types, included, skipCorrupt);
      default:
        throw new IllegalArgumentException("Unsupported type " +
            type.getKind());
    }
  }
}
