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
package org.apache.orc.impl;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.orc.TypeDescription;
import org.apache.orc.OrcProto;

/**
 * Factory for creating ORC tree readers.
 */
public class TreeReaderFactory {

  public abstract static class TreeReader {
    protected final int columnId;
    protected BitFieldReader present = null;
    protected boolean valuePresent = false;
    protected int vectorColumnCount;

    TreeReader(int columnId) throws IOException {
      this(columnId, null);
    }

    protected TreeReader(int columnId, InStream in) throws IOException {
      this.columnId = columnId;
      if (in == null) {
        present = null;
        valuePresent = true;
      } else {
        present = new BitFieldReader(in, 1);
      }
      vectorColumnCount = -1;
    }

    void setVectorColumnCount(int vectorColumnCount) {
      this.vectorColumnCount = vectorColumnCount;
    }

    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    static IntegerReader createIntegerReader(OrcProto.ColumnEncoding.Kind kind,
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

    /**
     * Called at the top level to read into the given batch.
     * @param batch the batch to read into
     * @param batchSize the number of rows to read
     * @throws IOException
     */
    public void nextBatch(VectorizedRowBatch batch,
                          int batchSize) throws IOException {
      batch.cols[0].reset();
      batch.cols[0].ensureSize(batchSize, false);
      nextVector(batch.cols[0], null, batchSize);
    }

    /**
     * Populates the isNull vector array in the previousVector object based on
     * the present stream values. This function is called from all the child
     * readers, and they all set the values based on isNull field value.
     *
     * @param previous The columnVector object whose isNull value is populated
     * @param isNull Whether the each value was null at a higher level. If
     *               isNull is null, all values are non-null.
     * @param batchSize      Size of the column vector
     * @throws IOException
     */
    public void nextVector(ColumnVector previous,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      if (present != null || isNull != null) {
        // Set noNulls and isNull vector of the ColumnVector based on
        // present stream
        previous.noNulls = true;
        boolean allNull = true;
        for (int i = 0; i < batchSize; i++) {
          if (isNull == null || !isNull[i]) {
            if (present != null && present.next() != 1) {
              previous.noNulls = false;
              previous.isNull[i] = true;
            } else {
              previous.isNull[i] = false;
              allNull = false;
            }
          } else {
            previous.noNulls = false;
            previous.isNull[i] = true;
          }
        }
        previous.isRepeating = !previous.noNulls && allNull;
      } else {
        // There is no present stream, this means that all the values are
        // present.
        previous.noNulls = true;
        for (int i = 0; i < batchSize; i++) {
          previous.isNull[i] = false;
        }
      }
    }

    public BitFieldReader getPresent() {
      return present;
    }
  }

  public static class NullTreeReader extends TreeReader {

    public NullTreeReader(int columnId) throws IOException {
      super(columnId);
    }

    @Override
    public void startStripe(Map<StreamName, InStream> streams,
                            OrcProto.StripeFooter footer) {
      // PASS
    }

    @Override
    void skipRows(long rows) {
      // PASS
    }

    @Override
    public void seek(PositionProvider position) {
      // PASS
    }

    @Override
    public void seek(PositionProvider[] position) {
      // PASS
    }

    @Override
    public void nextVector(ColumnVector vector, boolean[] isNull, int size) {
      vector.noNulls = false;
      vector.isNull[0] = true;
      vector.isRepeating = true;
    }
  }

  public static class BooleanTreeReader extends TreeReader {
    protected BitFieldReader reader = null;

    BooleanTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    protected BooleanTreeReader(int columnId, InStream present, InStream data) throws IOException {
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      LongColumnVector result = (LongColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
    }
  }

  public static class ByteTreeReader extends TreeReader {
    protected RunLengthByteReader reader = null;

    ByteTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    protected ByteTreeReader(int columnId, InStream present, InStream data) throws IOException {
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      final LongColumnVector result = (LongColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, result.vector, batchSize);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class ShortTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    ShortTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null);
    }

    protected ShortTreeReader(int columnId, InStream present, InStream data,
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      final LongColumnVector result = (LongColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, result.vector, batchSize);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class IntTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    IntTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null);
    }

    protected IntTreeReader(int columnId, InStream present, InStream data,
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      final LongColumnVector result = (LongColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, result.vector, batchSize);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class LongTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    LongTreeReader(int columnId, boolean skipCorrupt) throws IOException {
      this(columnId, null, null, null, skipCorrupt);
    }

    protected LongTreeReader(int columnId, InStream present, InStream data,
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      final LongColumnVector result = (LongColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, result.vector, batchSize);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class FloatTreeReader extends TreeReader {
    protected InStream stream;
    private final SerializationUtils utils;

    FloatTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    protected FloatTreeReader(int columnId, InStream present, InStream data) throws IOException {
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      final DoubleColumnVector result = (DoubleColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

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
    }

    @Override
    protected void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      for (int i = 0; i < items; ++i) {
        utils.readFloat(stream);
      }
    }
  }

  public static class DoubleTreeReader extends TreeReader {
    protected InStream stream;
    private final SerializationUtils utils;

    DoubleTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    protected DoubleTreeReader(int columnId, InStream present, InStream data) throws IOException {
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      final DoubleColumnVector result = (DoubleColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

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

  public static class BinaryTreeReader extends TreeReader {
    protected InStream stream;
    protected IntegerReader lengths = null;
    protected final LongColumnVector scratchlcv;

    BinaryTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null, null);
    }

    protected BinaryTreeReader(int columnId, InStream present, InStream data, InStream length,
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      final BytesColumnVector result = (BytesColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

      BytesColumnVectorUtil.readOrcByteArrays(stream, lengths, scratchlcv, result, batchSize);
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

  public static class TimestampTreeReader extends TreeReader {
    protected IntegerReader data = null;
    protected IntegerReader nanos = null;
    private final boolean skipCorrupt;
    private Map<String, Long> baseTimestampMap;
    protected long base_timestamp;
    private final TimeZone readerTimeZone;
    private TimeZone writerTimeZone;
    private boolean hasSameTZRules;

    TimestampTreeReader(int columnId, boolean skipCorrupt) throws IOException {
      this(columnId, null, null, null, null, skipCorrupt, null);
    }

    protected TimestampTreeReader(int columnId, InStream presentStream, InStream dataStream,
        InStream nanosStream, OrcProto.ColumnEncoding encoding, boolean skipCorrupt, String writerTimezone)
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
        base_timestamp = getBaseTimestamp(writerTimezone);
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

    protected long getBaseTimestamp(String timeZoneId) throws IOException {
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      TimestampColumnVector result = (TimestampColumnVector) previousVector;
      super.nextVector(previousVector, isNull, batchSize);

      for (int i = 0; i < batchSize; i++) {
        if (result.noNulls || !result.isNull[i]) {
          long millis = data.next() + base_timestamp;
          int newNanos = parseNanos(nanos.next());
          if (millis < 0 && newNanos != 0) {
            millis -= 1;
          }
          millis *= WriterImpl.MILLIS_PER_SECOND;
          long offset = 0;
          // If reader and writer time zones have different rules, adjust the timezone difference
          // between reader and writer taking day light savings into account.
          if (!hasSameTZRules) {
            offset = writerTimeZone.getOffset(millis) - readerTimeZone.getOffset(millis);
          }
          long adjustedMillis = millis + offset;
          // Sometimes the reader timezone might have changed after adding the adjustedMillis.
          // To account for that change, check for any difference in reader timezone after
          // adding adjustedMillis. If so use the new offset (offset at adjustedMillis point of time).
          if (!hasSameTZRules &&
              (readerTimeZone.getOffset(millis) != readerTimeZone.getOffset(adjustedMillis))) {
            long newOffset =
                writerTimeZone.getOffset(millis) - readerTimeZone.getOffset(adjustedMillis);
            adjustedMillis = millis + newOffset;
          }
          result.time[i] = adjustedMillis;
          result.nanos[i] = newNanos;
          if (result.isRepeating && i != 0 &&
              (result.time[0] != result.time[i] ||
                  result.nanos[0] != result.nanos[i])) {
            result.isRepeating = false;
          }
        }
      }
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

  public static class DateTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    DateTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null);
    }

    protected DateTreeReader(int columnId, InStream present, InStream data,
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      final LongColumnVector result = (LongColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, result.vector, batchSize);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class DecimalTreeReader extends TreeReader {
    protected InStream valueStream;
    protected IntegerReader scaleReader = null;
    private int[] scratchScaleVector;

    private final int precision;
    private final int scale;

    DecimalTreeReader(int columnId, int precision, int scale) throws IOException {
      this(columnId, precision, scale, null, null, null, null);
    }

    protected DecimalTreeReader(int columnId, int precision, int scale, InStream present,
        InStream valueStream, InStream scaleStream, OrcProto.ColumnEncoding encoding)
        throws IOException {
      super(columnId, present);
      this.precision = precision;
      this.scale = scale;
      this.scratchScaleVector = new int[VectorizedRowBatch.DEFAULT_SIZE];
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      final DecimalColumnVector result = (DecimalColumnVector) previousVector;
      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

      if (batchSize > scratchScaleVector.length) {
        scratchScaleVector = new int[(int) batchSize];
      }
      // read the scales
      scaleReader.nextVector(result, scratchScaleVector, batchSize);
      // Read value entries based on isNull entries
      if (result.noNulls) {
        for (int r=0; r < batchSize; ++r) {
          BigInteger bInt = SerializationUtils.readBigInteger(valueStream);
          HiveDecimal dec = HiveDecimal.create(bInt, scratchScaleVector[r]);
          result.set(r, dec);
        }
      } else if (!result.isRepeating || !result.isNull[0]) {
        for (int r=0; r < batchSize; ++r) {
          if (!result.isNull[r]) {
            BigInteger bInt = SerializationUtils.readBigInteger(valueStream);
            HiveDecimal dec = HiveDecimal.create(bInt, scratchScaleVector[r]);
            result.set(r, dec);
          }
        }
      }
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
  public static class StringTreeReader extends TreeReader {
    protected TreeReader reader;

    StringTreeReader(int columnId) throws IOException {
      super(columnId);
    }

    protected StringTreeReader(int columnId, InStream present, InStream data, InStream length,
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      reader.nextVector(previousVector, isNull, batchSize);
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
        BytesColumnVector result, final int batchSize) throws IOException {
      // Read lengths
      scratchlcv.isNull = result.isNull;  // Notice we are replacing the isNull vector here...
      scratchlcv.ensureSize(batchSize, false);
      lengths.nextVector(scratchlcv, scratchlcv.vector, batchSize);
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
    public static void readOrcByteArrays(InStream stream,
                                         IntegerReader lengths,
                                         LongColumnVector scratchlcv,
                                         BytesColumnVector result,
                                         final int batchSize) throws IOException {
      if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
        byte[] allBytes = commonReadByteArrays(stream, lengths, scratchlcv,
            result, (int) batchSize);

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
  }

  /**
   * A reader for string columns that are direct encoded in the current
   * stripe.
   */
  public static class StringDirectTreeReader extends TreeReader {
    private static final HadoopShims SHIMS = HadoopShims.Factory.get();
    protected InStream stream;
    protected HadoopShims.TextReaderShim data;
    protected IntegerReader lengths;
    private final LongColumnVector scratchlcv;

    StringDirectTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null, null);
    }

    protected StringDirectTreeReader(int columnId, InStream present, InStream data,
        InStream length, OrcProto.ColumnEncoding.Kind encoding) throws IOException {
      super(columnId, present);
      this.scratchlcv = new LongColumnVector();
      this.stream = data;
      if (length != null && encoding != null) {
        this.lengths = createIntegerReader(encoding, length, false, false);
        this.data = SHIMS.getTextReaderShim(this.stream);
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
      data = SHIMS.getTextReaderShim(this.stream);
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
      // don't seek data stream
      lengths.seek(index);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      final BytesColumnVector result = (BytesColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

      BytesColumnVectorUtil.readOrcByteArrays(stream, lengths, scratchlcv,
          result, batchSize);
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

    public IntegerReader getLengths() {
      return lengths;
    }

    public InStream getStream() {
      return stream;
    }
  }

  /**
   * A reader for string columns that are dictionary encoded in the current
   * stripe.
   */
  public static class StringDictionaryTreeReader extends TreeReader {
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private DynamicByteArray dictionaryBuffer;
    private int[] dictionaryOffsets;
    protected IntegerReader reader;

    private byte[] dictionaryBufferInBytesCache = null;
    private final LongColumnVector scratchlcv;

    StringDictionaryTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null, null, null);
    }

    protected StringDictionaryTreeReader(int columnId, InStream present, InStream data,
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      final BytesColumnVector result = (BytesColumnVector) previousVector;
      int offset;
      int length;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize);

      if (dictionaryBuffer != null) {

        // Load dictionaryBuffer into cache.
        if (dictionaryBufferInBytesCache == null) {
          dictionaryBufferInBytesCache = dictionaryBuffer.get();
        }

        // Read string offsets
        scratchlcv.isNull = result.isNull;
        scratchlcv.ensureSize((int) batchSize, false);
        reader.nextVector(scratchlcv, scratchlcv.vector, batchSize);
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
        if (dictionaryOffsets == null) {
          // Entire stripe contains null strings.
          result.isRepeating = true;
          result.noNulls = false;
          result.isNull[0] = true;
          result.setRef(0, EMPTY_BYTE_ARRAY, 0, 0);
        } else {
          // stripe contains nulls and empty strings
          for (int i = 0; i < batchSize; i++) {
            if (!result.isNull[i]) {
              result.setRef(i, EMPTY_BYTE_ARRAY, 0, 0);
            }
          }
        }
      }
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

    public IntegerReader getReader() {
      return reader;
    }
  }

  public static class CharTreeReader extends StringTreeReader {
    int maxLength;

    CharTreeReader(int columnId, int maxLength) throws IOException {
      this(columnId, maxLength, null, null, null, null, null);
    }

    protected CharTreeReader(int columnId, int maxLength, InStream present, InStream data,
        InStream length, InStream dictionary, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present, data, length, dictionary, encoding);
      this.maxLength = maxLength;
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      // Get the vector of strings from StringTreeReader, then make a 2nd pass to
      // adjust down the length (right trim and truncate) if necessary.
      super.nextVector(previousVector, isNull, batchSize);
      BytesColumnVector result = (BytesColumnVector) previousVector;
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
    }
  }

  public static class VarcharTreeReader extends StringTreeReader {
    int maxLength;

    VarcharTreeReader(int columnId, int maxLength) throws IOException {
      this(columnId, maxLength, null, null, null, null, null);
    }

    protected VarcharTreeReader(int columnId, int maxLength, InStream present, InStream data,
        InStream length, InStream dictionary, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present, data, length, dictionary, encoding);
      this.maxLength = maxLength;
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      // Get the vector of strings from StringTreeReader, then make a 2nd pass to
      // adjust down the length (truncate) if necessary.
      super.nextVector(previousVector, isNull, batchSize);
      BytesColumnVector result = (BytesColumnVector) previousVector;

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
    }
  }

  protected static class StructTreeReader extends TreeReader {
    protected final TreeReader[] fields;

    protected StructTreeReader(int columnId,
                               TypeDescription readerSchema,
                               SchemaEvolution evolution,
                               boolean[] included,
                               boolean skipCorrupt) throws IOException {
      super(columnId);

      List<TypeDescription> childrenTypes = readerSchema.getChildren();
      this.fields = new TreeReader[childrenTypes.size()];
      for (int i = 0; i < fields.length; ++i) {
        TypeDescription subtype = childrenTypes.get(i);
        this.fields[i] = createTreeReader(subtype, evolution, included, skipCorrupt);
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
    public void nextBatch(VectorizedRowBatch batch,
                          int batchSize) throws IOException {
      for(int i=0; i < fields.length &&
          (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
        batch.cols[i].reset();
        batch.cols[i].ensureSize((int) batchSize, false);
        fields[i].nextVector(batch.cols[i], null, batchSize);
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      super.nextVector(previousVector, isNull, batchSize);
      StructColumnVector result = (StructColumnVector) previousVector;
      if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
        result.isRepeating = false;

        // Read all the members of struct as column vectors
        boolean[] mask = result.noNulls ? null : result.isNull;
        for (int f = 0; f < fields.length; f++) {
          if (fields[f] != null) {
            fields[f].nextVector(result.fields[f], mask, batchSize);
          }
        }
      }
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

  public static class UnionTreeReader extends TreeReader {
    protected final TreeReader[] fields;
    protected RunLengthByteReader tags;

    protected UnionTreeReader(int fileColumn,
                              TypeDescription readerSchema,
                              SchemaEvolution evolution,
                              boolean[] included,
                              boolean skipCorrupt) throws IOException {
      super(fileColumn);
      List<TypeDescription> childrenTypes = readerSchema.getChildren();
      int fieldCount = childrenTypes.size();
      this.fields = new TreeReader[fieldCount];
      for (int i = 0; i < fieldCount; ++i) {
        TypeDescription subtype = childrenTypes.get(i);
        this.fields[i] = createTreeReader(subtype, evolution, included, skipCorrupt);
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
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      UnionColumnVector result = (UnionColumnVector) previousVector;
      super.nextVector(result, isNull, batchSize);
      if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
        result.isRepeating = false;
        tags.nextVector(result.noNulls ? null : result.isNull, result.tags,
            batchSize);
        boolean[] ignore = new boolean[(int) batchSize];
        for (int f = 0; f < result.fields.length; ++f) {
          // build the ignore list for this tag
          for (int r = 0; r < batchSize; ++r) {
            ignore[r] = (!result.noNulls && result.isNull[r]) ||
                result.tags[r] != f;
          }
          fields[f].nextVector(result.fields[f], ignore, batchSize);
        }
      }
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

  public static class ListTreeReader extends TreeReader {
    protected final TreeReader elementReader;
    protected IntegerReader lengths = null;

    protected ListTreeReader(int fileColumn,
                             TypeDescription readerSchema,
                             SchemaEvolution evolution,
                             boolean[] included,
                             boolean skipCorrupt) throws IOException {
      super(fileColumn);
      TypeDescription elementType = readerSchema.getChildren().get(0);
      elementReader = createTreeReader(elementType, evolution, included,
          skipCorrupt);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      lengths.seek(index[columnId]);
      elementReader.seek(index);
    }

    @Override
    public void nextVector(ColumnVector previous,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      ListColumnVector result = (ListColumnVector) previous;
      super.nextVector(result, isNull, batchSize);
      // if we have some none-null values, then read them
      if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
        lengths.nextVector(result, result.lengths, batchSize);
        // even with repeating lengths, the list doesn't repeat
        result.isRepeating = false;
        // build the offsets vector and figure out how many children to read
        result.childCount = 0;
        for (int r = 0; r < batchSize; ++r) {
          if (result.noNulls || !result.isNull[r]) {
            result.offsets[r] = result.childCount;
            result.childCount += result.lengths[r];
          }
        }
        result.child.ensureSize(result.childCount, false);
        elementReader.nextVector(result.child, null, result.childCount);
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

  public static class MapTreeReader extends TreeReader {
    protected final TreeReader keyReader;
    protected final TreeReader valueReader;
    protected IntegerReader lengths = null;

    protected MapTreeReader(int fileColumn,
                            TypeDescription readerSchema,
                            SchemaEvolution evolution,
                            boolean[] included,
                            boolean skipCorrupt) throws IOException {
      super(fileColumn);
      TypeDescription keyType = readerSchema.getChildren().get(0);
      TypeDescription valueType = readerSchema.getChildren().get(1);
      keyReader = createTreeReader(keyType, evolution, included, skipCorrupt);
      valueReader = createTreeReader(valueType, evolution, included, skipCorrupt);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      lengths.seek(index[columnId]);
      keyReader.seek(index);
      valueReader.seek(index);
    }

    @Override
    public void nextVector(ColumnVector previous,
                           boolean[] isNull,
                           final int batchSize) throws IOException {
      MapColumnVector result = (MapColumnVector) previous;
      super.nextVector(result, isNull, batchSize);
      if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
        lengths.nextVector(result, result.lengths, batchSize);
        // even with repeating lengths, the map doesn't repeat
        result.isRepeating = false;
        // build the offsets vector and figure out how many children to read
        result.childCount = 0;
        for (int r = 0; r < batchSize; ++r) {
          if (result.noNulls || !result.isNull[r]) {
            result.offsets[r] = result.childCount;
            result.childCount += result.lengths[r];
          }
        }
        result.keys.ensureSize(result.childCount, false);
        result.values.ensureSize(result.childCount, false);
        keyReader.nextVector(result.keys, null, result.childCount);
        valueReader.nextVector(result.values, null, result.childCount);
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

  public static TreeReader createTreeReader(TypeDescription readerType,
                                            SchemaEvolution evolution,
                                            boolean[] included,
                                            boolean skipCorrupt
                                            ) throws IOException {
    TypeDescription fileType = evolution.getFileType(readerType);
    if (fileType == null ||
        (included != null && !included[readerType.getId()])) {
      return new NullTreeReader(0);
    }
    TypeDescription.Category readerTypeCategory = readerType.getCategory();
    if (!fileType.equals(readerType) &&
        (readerTypeCategory != TypeDescription.Category.STRUCT &&
         readerTypeCategory != TypeDescription.Category.MAP &&
         readerTypeCategory != TypeDescription.Category.LIST &&
         readerTypeCategory != TypeDescription.Category.UNION)) {
      // We only convert complex children.
      return ConvertTreeReaderFactory.createConvertTreeReader(readerType, evolution,
          included, skipCorrupt);
    }
    switch (readerTypeCategory) {
      case BOOLEAN:
        return new BooleanTreeReader(fileType.getId());
      case BYTE:
        return new ByteTreeReader(fileType.getId());
      case DOUBLE:
        return new DoubleTreeReader(fileType.getId());
      case FLOAT:
        return new FloatTreeReader(fileType.getId());
      case SHORT:
        return new ShortTreeReader(fileType.getId());
      case INT:
        return new IntTreeReader(fileType.getId());
      case LONG:
        return new LongTreeReader(fileType.getId(), skipCorrupt);
      case STRING:
        return new StringTreeReader(fileType.getId());
      case CHAR:
        return new CharTreeReader(fileType.getId(), readerType.getMaxLength());
      case VARCHAR:
        return new VarcharTreeReader(fileType.getId(), readerType.getMaxLength());
      case BINARY:
        return new BinaryTreeReader(fileType.getId());
      case TIMESTAMP:
        return new TimestampTreeReader(fileType.getId(), skipCorrupt);
      case DATE:
        return new DateTreeReader(fileType.getId());
      case DECIMAL:
        return new DecimalTreeReader(fileType.getId(), readerType.getPrecision(),
            readerType.getScale());
      case STRUCT:
        return new StructTreeReader(fileType.getId(), readerType,
            evolution, included, skipCorrupt);
      case LIST:
        return new ListTreeReader(fileType.getId(), readerType,
            evolution, included, skipCorrupt);
      case MAP:
        return new MapTreeReader(fileType.getId(), readerType, evolution,
            included, skipCorrupt);
      case UNION:
        return new UnionTreeReader(fileType.getId(), readerType,
            evolution, included, skipCorrupt);
      default:
        throw new IllegalArgumentException("Unsupported type " +
            readerTypeCategory);
    }
  }
}
