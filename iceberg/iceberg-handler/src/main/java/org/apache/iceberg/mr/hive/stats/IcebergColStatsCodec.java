/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.stats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.function.IntPredicate;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

/**
 * How one column's statistics are written as an entry of a blob, and read back.
 *
 * <p>An entry is a Thrift struct, so it is written as one: the compact protocol states a field by
 * its id, which makes an entry a fraction of the size and a read a fraction of the work of the Java
 * serialization the first version of the blob used - that repeated the whole class descriptor in
 * every entry of every partition. Reading it back stays: a file written before this is still read
 * the way it was written.
 *
 * <p>Thrift is also what makes the blob safe to keep: a field the writer did not know is skipped
 * and one it did not write defaults, so an entry written by another version reads rather than
 * throwing, which is not true of a serialized Java class.
 */
final class IcebergColStatsCodec {

  private static final ThreadLocal<TSerializer> WRITERS = ThreadLocal.withInitial(() -> {
    try {
      return new TSerializer(new TCompactProtocol.Factory());
    } catch (TException e) {
      throw new IllegalStateException("Cannot write column statistics", e);
    }
  });

  private static final ThreadLocal<TDeserializer> READERS = ThreadLocal.withInitial(() -> {
    try {
      return new TDeserializer(new TCompactProtocol.Factory());
    } catch (TException e) {
      throw new IllegalStateException("Cannot read column statistics", e);
    }
  });

  /** What a blob holding more than one entry is written as, so a read knows what it is reading. */
  static final int BLOB_VERSION = 1;

  private IcebergColStatsCodec() {
  }

  /**
   * Entries one after another, each behind the field it is for and its own length. The field is
   * what lets a read take the columns it asked about and step over the rest, and it is written per
   * entry rather than once for the blob because a merge carries partitions gathered separately -
   * they need not hold the same columns, nor hold them in the same order.
   */
  static byte[] encodeBlob(List<byte[]> parts, List<Integer> fieldIds) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream data = new DataOutputStream(out);
    data.writeInt(BLOB_VERSION);
    data.writeInt(parts.size());
    for (int i = 0; i < parts.size(); i++) {
      data.writeInt(fieldIds.get(i));
      data.writeInt(parts.get(i).length);
      data.write(parts.get(i));
    }
    data.flush();
    return out.toByteArray();
  }

  /**
   * The entries the given places name, the rest skipped rather than copied. A scan of a wide table
   * asks about a few of its columns, and an entry it does not want costs a read nothing beyond the
   * length it steps over.
   */
  static List<byte[]> decodeBlob(ByteBuffer buf, IntPredicate wanted) {
    ByteBuffer data = buf.duplicate().order(ByteOrder.BIG_ENDIAN);
    if (data.remaining() < Integer.BYTES || data.getInt() != BLOB_VERSION) {
      return List.of();
    }
    int count = data.getInt();
    List<byte[]> parts = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      int fieldId = data.getInt();
      int length = data.getInt();
      if (wanted.test(fieldId)) {
        byte[] part = new byte[length];
        data.get(part);
        parts.add(part);
      } else {
        data.position(data.position() + length);
      }
    }
    return parts;
  }

  /** What the blob holds, or nothing where it was written in a shape this does not know. */
  static List<byte[]> decodeBlob(ByteBuffer buf) {
    return decodeBlob(buf, fieldId -> true);
  }

  /**
   * The entry as it is stored where nothing is stored beside it: itself, less the vector where
   * nothing will read it. A histogram stays, since one exists only where a statement was told to
   * compute it. The given entry is not touched.
   */
  static byte[] encodeEntry(ColumnStatisticsObj statsObj) throws IOException {
    ByteBuffer vector = sketchOf(statsObj.getStatsData());
    if (vector == null) {
      return serialize(statsObj);
    }
    // a merged entry holds its vector as the estimator the merge left behind, and the field
    // Thrift writes is empty until something asks for the bytes. Asking, and putting back what
    // is given, is what makes the vector something that can be stored at all
    ColumnStatisticsObj asStored = statsObj.deepCopy();
    setSketch(asStored.getStatsData(), vector);
    return serialize(asStored);
  }

  /**
   * A stored entry as the read asked for it: the vector only where something merges distinct
   * counts, the histogram always, since a plan reads one wherever it finds it.
   */
  static ColumnStatisticsObj decodeEntry(byte[] raw, boolean withVectors) {
    ColumnStatisticsObj statsObj = deserialize(raw);
    if (!withVectors && sketchOf(statsObj.getStatsData()) != null) {
      // the entry was decoded from bytes nothing else holds, so the vector goes without the copy
      // that dropping one from an entry a caller owns would need
      setSketch(statsObj.getStatsData(), null);
    }
    return statsObj;
  }

  /** The entry without the vector a distinct count is merged from. */
  static ColumnStatisticsObj withoutVectors(ColumnStatisticsObj statsObj) {
    if (sketchOf(statsObj.getStatsData()) == null) {
      return statsObj;
    }
    ColumnStatisticsObj without = statsObj.deepCopy();
    setSketch(without.getStatsData(), null);
    return without;
  }

  /** A boolean holds its own counts and a binary has no sketch, so neither carries one. */
  private static ByteBuffer sketchOf(ColumnStatisticsData data) {
    return switch (data.getSetField()) {
      case LONG_STATS -> data.getLongStats().bufferForBitVectors();
      case DOUBLE_STATS -> data.getDoubleStats().bufferForBitVectors();
      case STRING_STATS -> data.getStringStats().bufferForBitVectors();
      case DECIMAL_STATS -> data.getDecimalStats().bufferForBitVectors();
      case DATE_STATS -> data.getDateStats().bufferForBitVectors();
      case TIMESTAMP_STATS -> data.getTimestampStats().bufferForBitVectors();
      case null, default -> null;
    };
  }

  private static void setSketch(ColumnStatisticsData data, ByteBuffer sketch) {
    switch (data.getSetField()) {
      case LONG_STATS -> data.getLongStats().setBitVectors(sketch);
      case DOUBLE_STATS -> data.getDoubleStats().setBitVectors(sketch);
      case STRING_STATS -> data.getStringStats().setBitVectors(sketch);
      case DECIMAL_STATS -> data.getDecimalStats().setBitVectors(sketch);
      case DATE_STATS -> data.getDateStats().setBitVectors(sketch);
      case TIMESTAMP_STATS -> data.getTimestampStats().setBitVectors(sketch);
      // a boolean and a binary hold no vector to set
      case null, default -> { }
    }
  }

  /** The entry as the current blob stores it. */
  static byte[] serialize(ColumnStatisticsObj statsObj) throws IOException {
    try {
      return WRITERS.get().serialize(statsObj);
    } catch (TException e) {
      throw new IOException("Cannot write the statistics of " + statsObj.getColName(), e);
    }
  }

  /** The entry the given bytes hold. */
  static ColumnStatisticsObj deserialize(byte[] raw) {
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    try {
      READERS.get().deserialize(statsObj, raw);
    } catch (TException e) {
      throw new UncheckedIOException(new IOException("Cannot read a column's statistics", e));
    }
    return statsObj;
  }
}
