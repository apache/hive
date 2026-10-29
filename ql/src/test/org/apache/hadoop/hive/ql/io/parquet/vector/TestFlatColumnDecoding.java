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
package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.CalendarUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The batched flat-column path against the per-value path and against the written data: nulls at
 * batch edges, batches spanning pages, dictionary and PLAIN pages in one chunk, decimal64 and
 * HiveDecimal targets, precision narrowing, an all-null column, a constant column, page v1 and v2,
 * dates either side of the Julian/Gregorian switchover, booleans, and BINARY read as STRING, CHAR,
 * VARCHAR and BINARY including the empty string, multi-byte UTF-8 and CHAR/VARCHAR truncation.
 */
public class TestFlatColumnDecoding {

  private static final int ROWS = 20_000;
  private static final int[] BATCH_SIZES = { 1000, 1024, 333 };

  private static final MessageType SCHEMA = Types.buildMessage()
      .optional(PrimitiveTypeName.INT32).named("i32")
      .required(PrimitiveTypeName.INT32).named("i32r")
      .optional(PrimitiveTypeName.INT64).named("i64")
      .optional(PrimitiveTypeName.FLOAT).named("f")
      .optional(PrimitiveTypeName.DOUBLE).named("d")
      .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(4)
          .as(LogicalTypeAnnotation.decimalType(2, 7)).named("dec72")
      .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(8)
          .as(LogicalTypeAnnotation.decimalType(4, 18)).named("dec18")
      .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(5)
          .as(LogicalTypeAnnotation.decimalType(3, 11)).named("dec11")
      .optional(PrimitiveTypeName.INT32).named("allnull")
      .required(PrimitiveTypeName.INT32).named("constant")
      .required(PrimitiveTypeName.INT32).named("constant_plain")
      .optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.decimalType(2, 7)).named("dec32")
      .optional(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.decimalType(3, 15)).named("dec64")
      .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("s")
      .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("sr")
      .optional(PrimitiveTypeName.BINARY).named("bin")
      .optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named("dt")
      .required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named("dtr")
      .optional(PrimitiveTypeName.BOOLEAN).named("b")
      .required(PrimitiveTypeName.BOOLEAN).named("br")
      .named("m");

  private static final String[] COLUMNS =
      { "i32", "i32r", "i64", "f", "d", "dec72", "dec18", "dec11", "allnull", "constant", "constant_plain",
          "dec32", "dec64" };

  /** Written values per column (unscaled longs for decimals, raw bits for float/double). */
  private static final long[][] VALUES = new long[COLUMNS.length][ROWS];
  private static final boolean[][] NULLS = new boolean[COLUMNS.length][ROWS];

  private static final Configuration CONF = new Configuration();
  private static final Path V1_FILE = tempPath("flat-v1");
  private static final Path V2_FILE = tempPath("flat-v2");

  private static Path tempPath(String name) {
    return new Path(new File(System.getProperty("java.io.tmpdir"), name + "-" + System.nanoTime() + ".parquet").toURI());
  }

  @BeforeClass
  public static void writeFiles() throws IOException {
    generate();
    write(V1_FILE, WriterVersion.PARQUET_1_0);
    write(V2_FILE, WriterVersion.PARQUET_2_0);
  }

  @AfterClass
  public static void deleteFiles() throws IOException {
    for (Path p : List.of(V1_FILE, V2_FILE)) {
      p.getFileSystem(CONF).delete(p, false);
    }
  }

  private static void generate() {
    Random rnd = new Random(42);
    for (int r = 0; r < ROWS; r++) {
      for (int c = 0; c < COLUMNS.length; c++) {
        boolean optional = SCHEMA.getType(c).isRepetition(Type.Repetition.OPTIONAL);
        NULLS[c][r] = c == 8 || (optional && (r % 1000 == 0 || r % 1000 == 999 || rnd.nextInt(20) == 0));
      }
      VALUES[0][r] = rnd.nextInt(50);
      VALUES[1][r] = r < 5000 ? r % 100 : (r * 7919L) % 1_000_003L;
      VALUES[2][r] = rnd.nextInt(100) * 1_000_000_007L;
      VALUES[3][r] = Float.floatToRawIntBits(rnd.nextFloat() * 1000f);
      VALUES[4][r] = Double.doubleToRawLongBits(rnd.nextInt(100) * 1.25);
      VALUES[5][r] = (rnd.nextInt(100) - 50) * 12_345L;
      VALUES[6][r] = rnd.nextLong() % 1_000_000_000_000_000_000L;
      VALUES[7][r] = (rnd.nextInt(100) - 50) * 999_999_999L;
      VALUES[9][r] = 42;
      VALUES[10][r] = 42;
      VALUES[11][r] = (r * 7919L) % 9_999_999L - 4_999_999L;  // high cardinality: falls back to PLAIN
      VALUES[12][r] = rnd.nextLong() % 1_000_000_000_000_000L;
    }
  }

  /**
   * Small pages so batches span page boundaries; a small dictionary page so i32r, low cardinality at
   * first, starts dictionary encoded and falls back to PLAIN within the chunk. constant and
   * constant_plain are the columns whose batches come out repeating.
   */
  private static void write(Path path, WriterVersion version) throws IOException {
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withConf(CONF).withType(SCHEMA)
        .withWriterVersion(version).withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withDictionaryEncoding(true).withDictionaryEncoding("constant_plain", false)
        .withDictionaryPageSize(2048).withPageSize(4096).withRowGroupSize(64L << 20).build()) {
      SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
      for (int r = 0; r < ROWS; r++) {
        Group g = f.newGroup();
        for (int c = 0; c < COLUMNS.length; c++) {
          if (NULLS[c][r]) {
            continue;
          }
          long v = VALUES[c][r];
          switch (c) {
            case 0, 1, 9, 10, 11 -> g.append(COLUMNS[c], (int) v);
            case 2, 12 -> g.append(COLUMNS[c], v);
            case 3 -> g.append(COLUMNS[c], Float.intBitsToFloat((int) v));
            case 4 -> g.append(COLUMNS[c], Double.longBitsToDouble(v));
            case 5 -> g.append(COLUMNS[c], Binary.fromConstantByteArray(toFixedLenBytes(v, 4)));
            case 6 -> g.append(COLUMNS[c], Binary.fromConstantByteArray(toFixedLenBytes(v, 8)));
            case 7 -> g.append(COLUMNS[c], Binary.fromConstantByteArray(toFixedLenBytes(v, 5)));
            default -> throw new IllegalStateException();
          }
        }
        if (stringValue(r) != null) {
          g.append("s", stringValue(r));
        }
        g.append("sr", requiredStringValue(r));
        if (binaryValue(r) != null) {
          g.append("bin", Binary.fromConstantByteArray(binaryValue(r)));
        }
        if (!nullDate(r)) {
          g.append("dt", dateValue(r));
        }
        g.append("dtr", dateValue(r));
        if (!nullBool(r)) {
          g.append("b", boolValue(r));
        }
        g.append("br", boolValue(r));
        writer.write(g);
      }
    }
  }

  private static byte[] toFixedLenBytes(long value, int len) {
    byte[] bytes = new byte[len];
    if (value < 0) {
      Arrays.fill(bytes, (byte) 0xFF);
    }
    byte[] minimal = BigInteger.valueOf(value).toByteArray();
    System.arraycopy(minimal, 0, bytes, len - minimal.length, minimal.length);
    return bytes;
  }

  /**
   * The string column: low cardinality at first, so its dictionary falls back to PLAIN within the
   * chunk like i32r. NULL every 13th row, the empty string every 7th, a value whose UTF-8 runs two,
   * three and four bytes per character every 11th, and lengths that a CHAR(5) has to truncate.
   */
  private static String stringValue(int r) {
    if (r % 13 == 0) {
      return null;
    }
    if (r % 7 == 0) {
      return "";
    }
    String tail = Integer.toString(r < 5000 ? r % 100 : r);
    return r % 11 == 0 ? "\u00e9\u4e2d\ud83d\ude00" + tail : "value" + tail;
  }

  /** The same values in a required column, so the empty string also appears with no level stream. */
  private static String requiredStringValue(int r) {
    String value = stringValue(r);
    return value == null ? "" : value;
  }

  /** An unannotated BINARY column, so its bytes are not valid UTF-8 and never truncate. */
  private static byte[] binaryValue(int r) {
    if (r % 17 == 0) {
      return null;
    }
    int v = r < 5000 ? r % 50 : r;
    return new byte[] { (byte) 0xFF, (byte) v, (byte) (v >>> 8), (byte) 0x80 };
  }

  /** Day numbers either side of the 1582 switchover, so the hybrid to proleptic shift is exercised. */
  private static int dateValue(int r) {
    return r < 5000 ? (r % 40) * -6000 : r * 40 - 300_000;
  }

  /** Long runs and alternating stretches, so an RLE boolean page carries runs and packed groups. */
  private static boolean boolValue(int r) {
    return (r / 64) % 3 == 0 || r % 2 == 0;
  }

  private static boolean nullDate(int r) {
    return r % 19 == 0;
  }

  private static boolean nullBool(int r) {
    return r % 23 == 0;
  }

  /** One batch as read: flags, null map, and the non-null values. */
  private record Batch(boolean noNulls, boolean isRepeating, boolean[] isNull, long[] longs, double[] doubles,
      HiveDecimal[] decimals, byte[][] bytes) {
  }

  private static ColumnVector newVector(TypeInfo hiveType, boolean decimal64, int size) {
    if (hiveType instanceof DecimalTypeInfo d) {
      return decimal64 ? new Decimal64ColumnVector(size, d.getPrecision(), d.getScale())
          : new DecimalColumnVector(size, d.getPrecision(), d.getScale());
    }
    return switch (((PrimitiveTypeInfo) hiveType).getPrimitiveCategory()) {
      case FLOAT, DOUBLE -> new DoubleColumnVector(size);
      case STRING, CHAR, VARCHAR, BINARY -> {
        BytesColumnVector bytes = new BytesColumnVector(size);
        bytes.init();
        yield bytes;
      }
      case DATE -> new DateColumnVector(size);
      default -> new LongColumnVector(size);
    };
  }

  private static List<Batch> read(Path path, String column, TypeInfo hiveType, boolean decimal64, int batchSize,
      boolean flat) throws IOException {
    return read(path, column, hiveType, decimal64, batchSize, flat, false);
  }

  private static List<Batch> read(Path path, String column, TypeInfo hiveType, boolean decimal64, int batchSize,
      boolean flat, boolean skipProlepticConversion) throws IOException {
    List<Batch> batches = new ArrayList<>();
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, CONF))) {
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      ColumnDescriptor descriptor = schema.getColumnDescription(new String[] { column });
      Type type = schema.getType(column);
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        VectorizedPrimitiveColumnReader columnReader = new VectorizedPrimitiveColumnReader(descriptor,
            pages.getPageReader(descriptor), false, null, skipProlepticConversion, true, type, hiveType, flat);
        assertEquals(flat, columnReader.usesFlatDecoding());
        long left = pages.getRowCount();
        while (left > 0) {
          int n = (int) Math.min(batchSize, left);
          ColumnVector cv = newVector(hiveType, decimal64, batchSize);
          cv.reset();
          cv.isRepeating = true;
          columnReader.readBatch(n, cv, hiveType);
          batches.add(snapshot(cv, n));
          left -= n;
        }
      }
    }
    return batches;
  }

  /** One readBatch of zero rows on a fresh reader: the flags the caller set must survive it. */
  private static Batch readEmptyBatch(Path path, String column, TypeInfo hiveType) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, CONF))) {
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      ColumnDescriptor descriptor = schema.getColumnDescription(new String[] { column });
      PageReadStore pages = reader.readNextRowGroup();
      VectorizedPrimitiveColumnReader columnReader = new VectorizedPrimitiveColumnReader(descriptor,
          pages.getPageReader(descriptor), false, null, false, true, schema.getType(column), hiveType, true);
      ColumnVector cv = newVector(hiveType, false, 16);
      cv.reset();
      cv.isRepeating = true;
      columnReader.readBatch(0, cv, hiveType);
      return snapshot(cv, 0);
    }
  }

  private static Batch snapshot(ColumnVector cv, int n) {
    boolean[] isNull = Arrays.copyOf(cv.isNull, n);
    long[] longs = null;
    double[] doubles = null;
    HiveDecimal[] decimals = null;
    byte[][] bytes = null;
    if (cv instanceof LongColumnVector c) {
      longs = Arrays.copyOf(c.vector, n);
    } else if (cv instanceof DoubleColumnVector c) {
      doubles = Arrays.copyOf(c.vector, n);
    } else if (cv instanceof BytesColumnVector c) {
      bytes = new byte[n][];
      for (int i = 0; i < n; i++) {
        bytes[i] = isNull[i] ? null : Arrays.copyOfRange(c.vector[i], c.start[i], c.start[i] + c.length[i]);
      }
    } else {
      DecimalColumnVector c = (DecimalColumnVector) cv;
      decimals = new HiveDecimal[n];
      for (int i = 0; i < n; i++) {
        decimals[i] = isNull[i] ? null : c.vector[i].getHiveDecimal();
      }
    }
    return new Batch(cv.noNulls, cv.isRepeating, isNull, longs, doubles, decimals, bytes);
  }

  private static boolean constant(Batch b) {
    for (int i = 1; i < b.isNull.length; i++) {
      boolean same = b.longs != null ? b.longs[i] == b.longs[0]
          : b.doubles != null ? b.doubles[i] == b.doubles[0]
              : b.decimals != null ? b.decimals[i].equals(b.decimals[0]) : Arrays.equals(b.bytes[i], b.bytes[0]);
      if (!same) {
        return false;
      }
    }
    return true;
  }

  private static void assertSameBatches(String label, List<Batch> expected, List<Batch> actual) {
    assertEquals(label, expected.size(), actual.size());
    for (int b = 0; b < expected.size(); b++) {
      Batch e = expected.get(b);
      Batch a = actual.get(b);
      String at = label + " batch " + b;
      assertEquals(at + " noNulls", e.noNulls, a.noNulls);
      // The batched path may report a dictionary batch repeating where the per-value path never does.
      if (e.isRepeating || !a.isRepeating) {
        assertEquals(at + " isRepeating", e.isRepeating, a.isRepeating);
      } else {
        assertTrue(at + " repeating with nulls", a.noNulls);
        assertTrue(at + " repeating but not constant", constant(e));
      }
      assertTrue(at + " isNull", Arrays.equals(e.isNull, a.isNull));
      for (int i = 0; i < e.isNull.length; i++) {
        if (e.isNull[i]) {
          continue;
        }
        if (e.longs != null) {
          assertEquals(at + " row " + i, e.longs[i], a.longs[i]);
        } else if (e.doubles != null) {
          assertEquals(at + " row " + i, Double.doubleToRawLongBits(e.doubles[i]),
              Double.doubleToRawLongBits(a.doubles[i]));
        } else if (e.bytes != null) {
          assertArrayEquals(at + " row " + i, e.bytes[i], a.bytes[i]);
        } else {
          assertEquals(at + " row " + i, e.decimals[i], a.decimals[i]);
        }
      }
    }
  }

  /**
   * Compare against the written values; {@code absMax} NULLs the decimal64 values a narrowed Hive
   * precision cannot hold, the way the reader does.
   */
  private static void assertWrittenValues(String label, int col, List<Batch> batches, long absMax) {
    int row = 0;
    for (Batch b : batches) {
      boolean sawNull = false;
      for (int i = 0; i < b.isNull.length; i++, row++) {
        long v = VALUES[col][row];
        boolean isNull = NULLS[col][row] || (absMax > 0 && (v < -absMax || v > absMax));
        sawNull |= isNull;
        assertEquals(label + " isNull at " + row, isNull, b.isNull[i]);
        if (isNull) {
          continue;
        }
        if (b.longs != null) {
          assertEquals(label + " at " + row, v, b.longs[i]);
        } else if (col == 3) {
          assertEquals(label + " at " + row, Float.intBitsToFloat((int) v), b.doubles[i], 0.0);
        } else {
          assertEquals(label + " at " + row, Double.longBitsToDouble(v), b.doubles[i], 0.0);
        }
      }
      assertEquals(label + " noNulls", !sawNull, b.noNulls);
      assertEquals(label + " isRepeating", col == 9 || col == 10, b.isRepeating);
    }
    assertEquals(ROWS, row);
  }

  private void verify(Path file, String version) throws IOException {
    for (int batchSize : BATCH_SIZES) {
      for (int c = 0; c < COLUMNS.length; c++) {
        String column = COLUMNS[c];
        String label = version + " " + column + " batch " + batchSize;
        TypeInfo hiveType = switch (column) {
          case "i32r", "i64" -> TypeInfoFactory.longTypeInfo;
          case "f" -> TypeInfoFactory.floatTypeInfo;
          case "d" -> TypeInfoFactory.doubleTypeInfo;
          case "dec72" -> TypeInfoFactory.getDecimalTypeInfo(7, 2);
          case "dec18" -> TypeInfoFactory.getDecimalTypeInfo(18, 4);
          case "dec11" -> TypeInfoFactory.getDecimalTypeInfo(11, 3);
          case "dec32" -> TypeInfoFactory.getDecimalTypeInfo(7, 2);
          case "dec64" -> TypeInfoFactory.getDecimalTypeInfo(15, 3);
          default -> TypeInfoFactory.intTypeInfo;
        };
        boolean decimal = hiveType instanceof DecimalTypeInfo;
        List<Batch> flat = read(file, column, hiveType, decimal, batchSize, true);
        List<Batch> perValue = read(file, column, hiveType, decimal, batchSize, false);
        assertSameBatches(label, perValue, flat);
        assertWrittenValues(label, c, flat, 0);
        if (decimal) {
          assertSameBatches(label + " HiveDecimal", read(file, column, hiveType, false, batchSize, false),
              read(file, column, hiveType, false, batchSize, true));
        }
      }
    }
  }

  @Test
  public void testPageV1() throws IOException {
    verify(V1_FILE, "v1");
  }

  @Test
  public void testPageV2() throws IOException {
    verify(V2_FILE, "v2");
  }

  @Test
  public void testDictionaryAndPlainPagesInOneChunk() throws IOException {
    for (Path file : List.of(V1_FILE, V2_FILE)) {
      try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, CONF))) {
        for (String column : List.of("i32r", "s")) {
          Set<Encoding> encodings = chunk(reader, column).getEncodings();
          assertTrue(column + " " + encodings, encodings.contains(Encoding.PLAIN)
              || encodings.contains(Encoding.DELTA_BINARY_PACKED) || encodings.contains(Encoding.DELTA_BYTE_ARRAY));
          assertTrue(column + " " + encodings,
              encodings.contains(Encoding.PLAIN_DICTIONARY) || encodings.contains(Encoding.RLE_DICTIONARY));
        }
      }
    }
  }

  private static ColumnChunkMetaData chunk(ParquetFileReader reader, String column) {
    return reader.getFooter().getBlocks().get(0).getColumns().stream()
        .filter(c -> c.getPath().toDotString().equals(column)).findFirst().orElseThrow();
  }

  /**
   * DATE: INT32 day numbers into a DateColumnVector, shifted out of the hybrid Julian/Gregorian
   * calendar unless the file says the writer was already proleptic.
   */
  @Test
  public void testDate() throws IOException {
    for (Path file : List.of(V1_FILE, V2_FILE)) {
      for (int batchSize : BATCH_SIZES) {
        for (String column : List.of("dt", "dtr")) {
          for (boolean skipProleptic : List.of(false, true)) {
            String label = file.getName() + " " + column + " batch " + batchSize + " skipProleptic " + skipProleptic;
            TypeInfo date = TypeInfoFactory.dateTypeInfo;
            List<Batch> flat = read(file, column, date, false, batchSize, true, skipProleptic);
            assertSameBatches(label, read(file, column, date, false, batchSize, false, skipProleptic), flat);
            assertWrittenDates(label, column, flat, skipProleptic);
          }
        }
      }
    }
  }

  /**
   * BINARY into a BytesColumnVector: STRING and BINARY take the bytes whole, CHAR and VARCHAR
   * truncate to a character count, and an unannotated BINARY never truncates even into a CHAR.
   */
  @Test
  public void testBinary() throws IOException {
    List<TypeInfo> asText = List.of(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.getCharTypeInfo(5),
        TypeInfoFactory.getVarcharTypeInfo(5), TypeInfoFactory.binaryTypeInfo);
    for (Path file : List.of(V1_FILE, V2_FILE)) {
      for (int batchSize : BATCH_SIZES) {
        for (String column : List.of("s", "sr")) {
          for (TypeInfo hiveType : asText) {
            String label = file.getName() + " " + column + " as " + hiveType.getTypeName() + " batch " + batchSize;
            List<Batch> flat = read(file, column, hiveType, false, batchSize, true);
            assertSameBatches(label, read(file, column, hiveType, false, batchSize, false), flat);
            assertWrittenStrings(label, column, hiveType, flat);
          }
        }
        for (TypeInfo hiveType : List.of(TypeInfoFactory.binaryTypeInfo, TypeInfoFactory.getCharTypeInfo(2))) {
          String label = file.getName() + " bin as " + hiveType.getTypeName() + " batch " + batchSize;
          List<Batch> flat = read(file, "bin", hiveType, false, batchSize, true);
          assertSameBatches(label, read(file, "bin", hiveType, false, batchSize, false), flat);
          assertWrittenBinaries(label, flat);
        }
      }
    }
  }

  /** BOOLEAN: bit-packed on a PLAIN page, a bit-width-1 hybrid stream on a page v2 one. */
  @Test
  public void testBoolean() throws IOException {
    for (Path file : List.of(V1_FILE, V2_FILE)) {
      for (int batchSize : BATCH_SIZES) {
        for (String column : List.of("b", "br")) {
          String label = file.getName() + " " + column + " batch " + batchSize;
          TypeInfo bool = TypeInfoFactory.booleanTypeInfo;
          List<Batch> flat = read(file, column, bool, false, batchSize, true);
          assertSameBatches(label, read(file, column, bool, false, batchSize, false), flat);
          assertWrittenBooleans(label, column, flat);
        }
      }
    }
  }

  /** A zero-row batch decodes nothing and leaves the flags the caller set, whatever the type. */
  @Test
  public void testEmptyBatch() throws IOException {
    List<String> columns = List.of("i32", "dec72", "s", "bin", "b", "dt");
    List<TypeInfo> types = List.of(TypeInfoFactory.intTypeInfo, TypeInfoFactory.getDecimalTypeInfo(7, 2),
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.binaryTypeInfo, TypeInfoFactory.booleanTypeInfo,
        TypeInfoFactory.dateTypeInfo);
    for (Path file : List.of(V1_FILE, V2_FILE)) {
      for (int c = 0; c < columns.size(); c++) {
        Batch empty = readEmptyBatch(file, columns.get(c), types.get(c));
        assertTrue(file.getName() + " empty " + columns.get(c) + " noNulls", empty.noNulls);
        assertTrue(file.getName() + " empty " + columns.get(c) + " isRepeating", empty.isRepeating);
        assertEquals(0, empty.isNull.length);
      }
    }
  }

  private static void assertWrittenDates(String label, String column, List<Batch> batches, boolean skipProleptic) {
    boolean optional = column.equals("dt");
    int row = 0;
    for (Batch b : batches) {
      for (int i = 0; i < b.isNull.length; i++, row++) {
        boolean isNull = optional && nullDate(row);
        assertEquals(label + " isNull at " + row, isNull, b.isNull[i]);
        if (!isNull) {
          int written = dateValue(row);
          assertEquals(label + " at " + row,
              skipProleptic ? written : CalendarUtils.convertDateToProleptic(written), b.longs[i]);
        }
      }
    }
    assertEquals(ROWS, row);
  }

  private static void assertWrittenStrings(String label, String column, TypeInfo hiveType, List<Batch> batches) {
    int max = hiveType instanceof BaseCharTypeInfo c ? c.getLength() : -1;
    boolean optional = column.equals("s");
    int row = 0;
    for (Batch b : batches) {
      for (int i = 0; i < b.isNull.length; i++, row++) {
        String written = optional ? stringValue(row) : requiredStringValue(row);
        assertEquals(label + " isNull at " + row, written == null, b.isNull[i]);
        if (written != null) {
          assertArrayEquals(label + " at " + row, utf8(written, max), b.bytes[i]);
        }
      }
    }
    assertEquals(ROWS, row);
  }

  /** The first {@code max} code points of {@code value} as UTF-8; a non-positive max keeps it whole. */
  private static byte[] utf8(String value, int max) {
    int[] points = value.codePoints().toArray();
    String kept = max > 0 && points.length > max ? new String(points, 0, max) : value;
    return kept.getBytes(StandardCharsets.UTF_8);
  }

  private static void assertWrittenBinaries(String label, List<Batch> batches) {
    int row = 0;
    for (Batch b : batches) {
      for (int i = 0; i < b.isNull.length; i++, row++) {
        byte[] written = binaryValue(row);
        assertEquals(label + " isNull at " + row, written == null, b.isNull[i]);
        if (written != null) {
          assertArrayEquals(label + " at " + row, written, b.bytes[i]);
        }
      }
    }
    assertEquals(ROWS, row);
  }

  private static void assertWrittenBooleans(String label, String column, List<Batch> batches) {
    boolean optional = column.equals("b");
    int row = 0;
    for (Batch b : batches) {
      for (int i = 0; i < b.isNull.length; i++, row++) {
        boolean isNull = optional && nullBool(row);
        assertEquals(label + " isNull at " + row, isNull, b.isNull[i]);
        if (!isNull) {
          assertEquals(label + " at " + row, boolValue(row) ? 1L : 0L, b.longs[i]);
        }
      }
    }
    assertEquals(ROWS, row);
  }

  /**
   * Precision narrowing: DECIMAL(7,2) data read as DECIMAL(5,2). The decimal64 target NULLs the
   * out-of-range entries, the HiveDecimal target enforces precision per value; both match the
   * per-value path.
   */
  @Test
  public void testDecimalPrecisionNarrowing() throws IOException {
    DecimalTypeInfo narrow = TypeInfoFactory.getDecimalTypeInfo(5, 2);
    for (Path file : List.of(V1_FILE, V2_FILE)) {
      for (int batchSize : BATCH_SIZES) {
        List<Batch> flat = read(file, "dec72", narrow, true, batchSize, true);
        assertSameBatches("narrow decimal64", read(file, "dec72", narrow, true, batchSize, false), flat);
        assertWrittenValues("narrow decimal64", 5, flat, 99_999L);
        assertSameBatches("narrow HiveDecimal", read(file, "dec72", narrow, false, batchSize, false),
            read(file, "dec72", narrow, false, batchSize, true));
      }
    }
  }

  /**
   * Scale evolution keeps the decimal64 target on the per-value path.
   */
  @Test
  public void testScaleEvolutionFallsBack() throws IOException {
    DecimalTypeInfo rescaled = TypeInfoFactory.getDecimalTypeInfo(9, 4);
    List<Batch> flat = read(V1_FILE, "dec72", rescaled, true, 1024, true);
    assertSameBatches("rescaled decimal64", read(V1_FILE, "dec72", rescaled, true, 1024, false), flat);
    assertFalse(flat.get(0).noNulls);
    assertNotNull(flat.get(0).longs);
  }

  /**
   * The hybrid decoder against parquet's own, over every bit width and random run structure, read
   * in random slice sizes.
   */
  @Test
  public void testRleBitPackedDecoder() throws IOException {
    Random rnd = new Random(7);
    for (int bitWidth = 0; bitWidth <= 24; bitWidth++) {
      int max = (1 << bitWidth) - 1;
      for (int round = 0; round < 4; round++) {
        int n = 1 + rnd.nextInt(5000);
        int[] values = new int[n];
        int i = 0;
        while (i < n) {
          int run = 1 + rnd.nextInt(40);
          boolean repeat = rnd.nextBoolean();
          int v = max == 0 ? 0 : rnd.nextInt(max + 1);
          for (int k = 0; k < run && i < n; k++, i++) {
            values[i] = repeat ? v : (max == 0 ? 0 : rnd.nextInt(max + 1));
          }
        }
        byte[] bytes;
        try (RunLengthBitPackingHybridEncoder encoder =
            new RunLengthBitPackingHybridEncoder(bitWidth, 64, 64 << 10, new HeapByteBufferAllocator())) {
          for (int v : values) {
            encoder.writeInt(v);
          }
          bytes = encoder.toBytes().toByteArray();
        }
        RunLengthBitPackingHybridDecoder reference =
            new RunLengthBitPackingHybridDecoder(bitWidth, new ByteArrayInputStream(bytes));
        RleBitPackedIntDecoder decoder = new RleBitPackedIntDecoder(bitWidth);
        decoder.reset(ByteBuffer.wrap(bytes), 0, bytes.length);
        int[] out = new int[n];
        for (int pos = 0; pos < n; ) {
          int slice = Math.min(n - pos, 1 + rnd.nextInt(300));
          decoder.read(out, pos, slice);
          pos += slice;
        }
        for (int k = 0; k < n; k++) {
          int expected = reference.readInt();
          assertEquals(values[k], expected);
          assertEquals("bitWidth " + bitWidth + " round " + round + " value " + k, expected, out[k]);
        }
      }
    }
  }
}
