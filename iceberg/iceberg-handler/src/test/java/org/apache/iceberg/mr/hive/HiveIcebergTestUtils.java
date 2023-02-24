/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.common.type.TimestampUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobID;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.junit.Assert;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class HiveIcebergTestUtils {
  // TODO: Can this be a constant all around the Iceberg tests?
  public static final Schema FULL_SCHEMA = new Schema(
      // TODO: Create tests for field case insensitivity.
      optional(1, "boolean_type", Types.BooleanType.get()),
      optional(2, "integer_type", Types.IntegerType.get()),
      optional(3, "long_type", Types.LongType.get()),
      optional(4, "float_type", Types.FloatType.get()),
      optional(5, "double_type", Types.DoubleType.get()),
      optional(6, "date_type", Types.DateType.get()),
      optional(7, "tstz", Types.TimestampType.withZone()),
      optional(8, "ts", Types.TimestampType.withoutZone()),
      optional(9, "string_type", Types.StringType.get()),
      optional(10, "fixed_type", Types.FixedType.ofLength(3)),
      optional(11, "binary_type", Types.BinaryType.get()),
      optional(12, "decimal_type", Types.DecimalType.of(38, 10)),
      optional(13, "time_type", Types.TimeType.get()),
      optional(14, "uuid_type", Types.UUIDType.get()));

  public static final StandardStructObjectInspector FULL_SCHEMA_OBJECT_INSPECTOR =
      ObjectInspectorFactory.getStandardStructObjectInspector(
          Arrays.asList("boolean_type", "integer_type", "long_type", "float_type", "double_type",
              "date_type", "tstz", "ts", "string_type", "fixed_type", "binary_type", "decimal_type",
              "time_type", "uuid_type"),
          Arrays.asList(
              PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
              PrimitiveObjectInspectorFactory.writableIntObjectInspector,
              PrimitiveObjectInspectorFactory.writableLongObjectInspector,
              PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
              PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
              PrimitiveObjectInspectorFactory.writableDateObjectInspector,
              PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
              PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
              PrimitiveObjectInspectorFactory.writableStringObjectInspector,
              PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
              PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
              PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector,
              PrimitiveObjectInspectorFactory.writableStringObjectInspector,
              PrimitiveObjectInspectorFactory.writableStringObjectInspector
          ));

  private HiveIcebergTestUtils() {
    // Empty constructor for the utility class
  }

  /**
   * Generates a test record where every field has a value.
   * @return Record with every field set
   */
  public static Record getTestRecord() {
    Record record = GenericRecord.create(HiveIcebergTestUtils.FULL_SCHEMA);
    record.set(0, true);
    record.set(1, 1);
    record.set(2, 2L);
    record.set(3, 3.1f);
    record.set(4, 4.2d);
    record.set(5, LocalDate.of(2020, 1, 21));
    // Nano is not supported ?
    record.set(6, OffsetDateTime.of(2017, 11, 22, 11, 30, 7, 0, ZoneOffset.ofHours(2)));
    record.set(7, LocalDateTime.of(2019, 2, 22, 9, 44, 54));
    record.set(8, "kilenc");
    record.set(9, new byte[]{0, 1, 2});
    record.set(10, ByteBuffer.wrap(new byte[]{0, 1, 2, 3}));
    record.set(11, new BigDecimal("0.0000000013"));
    record.set(12, LocalTime.of(11, 33));
    record.set(13, UUID.fromString("73689599-d7fc-4dfb-b94e-106ff20284a5"));

    return record;
  }

  /**
   * Record with every field set to null.
   * @return Empty record
   */
  public static Record getNullTestRecord() {
    Record record = GenericRecord.create(HiveIcebergTestUtils.FULL_SCHEMA);

    for (int i = 0; i < HiveIcebergTestUtils.FULL_SCHEMA.columns().size(); i++) {
      record.set(i, null);
    }

    return record;
  }

  /**
   * Hive values for the test record.
   * @param record The original Iceberg record
   * @return The Hive 'record' containing the same values
   */
  public static List<Object> valuesForTestRecord(Record record) {
    return Arrays.asList(
        new BooleanWritable(Boolean.TRUE),
        new IntWritable(record.get(1, Integer.class)),
        new LongWritable(record.get(2, Long.class)),
        new FloatWritable(record.get(3, Float.class)),
        new DoubleWritable(record.get(4, Double.class)),
        new DateWritable((int) record.get(5, LocalDate.class).toEpochDay()),
        new TimestampWritable(Timestamp.from(record.get(6, OffsetDateTime.class).toInstant())),
        new TimestampWritable(Timestamp.valueOf(record.get(7, LocalDateTime.class))),
        new Text(record.get(8, String.class)),
        new BytesWritable(record.get(9, byte[].class)),
        new BytesWritable(ByteBuffers.toByteArray(record.get(10, ByteBuffer.class))),
        new HiveDecimalWritable(HiveDecimal.create(record.get(11, BigDecimal.class))),
        new Text(record.get(12, LocalTime.class).toString()),
        new Text(record.get(13, UUID.class).toString())
    );
  }

  /**
   * Converts a list of Object arrays to a list of Iceberg records.
   * @param schema The schema of the Iceberg record
   * @param rows The data of the records
   * @return The list of the converted records
   */
  public static List<Record> valueForRow(Schema schema, List<Object[]> rows) {
    return rows.stream().map(row -> {
      Record record = GenericRecord.create(schema);
      for (int i = 0; i < row.length; ++i) {
        record.set(i, row[i]);
      }
      return record;
    }).collect(Collectors.toList());
  }

  /**
   * Check if 2 Iceberg records are the same or not. Compares OffsetDateTimes only by the Intant they represent.
   * @param expected The expected record
   * @param actual The actual record
   */
  public static void assertEquals(Record expected, Record actual) {
    for (int i = 0; i < expected.size(); ++i) {
      if (expected.get(i) instanceof OffsetDateTime) {
        // For OffsetDateTime we just compare the actual instant
        Assert.assertEquals(((OffsetDateTime) expected.get(i)).toInstant(),
            ((OffsetDateTime) actual.get(i)).toInstant());
      } else if (expected.get(i) instanceof byte[]) {
        Assert.assertArrayEquals((byte[]) expected.get(i), (byte[]) actual.get(i));
      } else {
        Assert.assertEquals(expected.get(i), actual.get(i));
      }
    }
  }

  /**
   * Validates whether the table contains the expected records. The results should be sorted by a unique key so we do
   * not end up with flaky tests.
   * @param table The table we should read the records from
   * @param expected The expected list of Records
   * @param sortBy The column position by which we will sort
   * @throws IOException Exceptions when reading the table data
   */
  public static void validateData(Table table, List<Record> expected, int sortBy) throws IOException {
    // Refresh the table, so we get the new data as well
    table.refresh();
    List<Record> records = Lists.newArrayListWithExpectedSize(expected.size());
    try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
      iterable.forEach(records::add);
    }

    validateData(expected, records, sortBy);
  }

  /**
   * Validates whether the 2 sets of records are the same. The results should be sorted by a unique key so we do
   * not end up with flaky tests.
   * @param expected The expected list of Records
   * @param actual The actual list of Records
   * @param sortBy The column position by which we will sort
   */
  public static void validateData(List<Record> expected, List<Record> actual, int sortBy) {
    List<Record> sortedExpected = Lists.newArrayList(expected);
    List<Record> sortedActual = Lists.newArrayList(actual);
    // Sort based on the specified column
    sortedExpected.sort(Comparator.comparingInt(record -> record.get(sortBy).hashCode()));
    sortedActual.sort(Comparator.comparingInt(record -> record.get(sortBy).hashCode()));

    Assert.assertEquals(sortedExpected.size(), sortedActual.size());
    validateData(sortedExpected, sortedActual);
  }

  /**
   * Validates whether the 2 sets of records are the same.
   * @param expected The expected list of Records
   * @param actual The actual list of Records
   */
  public static void validateData(List<Record> expected, List<Record> actual) {
    for (int i = 0; i < expected.size(); ++i) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  /**
   * Validates the number of files under a {@link Table} generated by a specific queryId and jobId.
   * Validates that the commit files are removed.
   * @param table The table we are checking
   * @param conf The configuration used for generating the job location
   * @param jobId The jobId which generated the files
   * @param dataFileNum The expected number of data files (TABLE_LOCATION/data/*)
   */
  public static void validateFiles(Table table, Configuration conf, JobID jobId, int dataFileNum) throws IOException {
    List<Path> dataFiles = Files.walk(Paths.get(table.location() + "/data"))
        .filter(Files::isRegularFile)
        .filter(path -> !path.getFileName().toString().startsWith("."))
        .collect(Collectors.toList());

    Assert.assertEquals(dataFileNum, dataFiles.size());
    Assert.assertFalse(
        new File(HiveIcebergOutputCommitter.generateJobLocation(table.location(), conf, jobId)).exists());
  }

  /**
   * Validates whether the table contains the expected records. The records are retrieved by Hive query and compared as
   * strings. The results should be sorted by a unique key so we do not end up with flaky tests.
   * @param shell Shell to execute the query
   * @param tableName The table to query
   * @param expected The expected list of Records
   * @param sortBy The column name by which we will sort
   */
  public static void validateDataWithSQL(TestHiveShell shell, String tableName, List<Record> expected, String sortBy) {
    List<Object[]> rows = shell.executeStatement("SELECT * FROM " + tableName + " ORDER BY " + sortBy);

    Assert.assertEquals(expected.size(), rows.size());
    for (int i = 0; i < expected.size(); ++i) {
      Object[] row = rows.get(i);
      Record record = expected.get(i);
      Assert.assertEquals(record.size(), row.length);
      for (int j = 0; j < record.size(); ++j) {
        Object field = record.get(j);
        if (field == null) {
          Assert.assertNull(row[j]);
        } else if (field instanceof LocalDateTime) {
          Assert.assertEquals(((LocalDateTime) field).toInstant(ZoneOffset.UTC).toEpochMilli(),
              TimestampUtils.stringToTimestamp((String) row[j]).toEpochMilli());
        } else if (field instanceof OffsetDateTime) {
          Assert.assertEquals(((OffsetDateTime) field).toInstant().toEpochMilli(),
              TimestampTZUtil.parse((String) row[j], ZoneId.systemDefault()).toEpochMilli());
        } else {
          Assert.assertEquals(field.toString(), row[j].toString());
        }
      }
    }
  }

  /**
   * @param table The table to create the delete file for
   * @param deleteFilePath The path where the delete file should be created, relative to the table location root
   * @param equalityFields List of field names that should play a role in the equality check
   * @param fileFormat The file format that should be used for writing out the delete file
   * @param rowsToDelete The rows that should be deleted. It's enough to fill out the fields that are relevant for the
   *                     equality check, as listed in equalityFields, the rest of the fields are ignored
   * @return The DeleteFile created
   * @throws IOException If there is an error during DeleteFile write
   */
  public static DeleteFile createEqualityDeleteFile(Table table, String deleteFilePath, List<String> equalityFields,
      FileFormat fileFormat, List<Record> rowsToDelete) throws IOException {
    List<Integer> equalityFieldIds = equalityFields.stream()
        .map(id -> table.schema().findField(id).fieldId())
        .collect(Collectors.toList());
    Schema eqDeleteRowSchema = table.schema().select(equalityFields.toArray(new String[]{}));

    FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(),
        ArrayUtil.toIntArray(equalityFieldIds), eqDeleteRowSchema, null);
    EncryptedOutputFile outputFile = table.encryption().encrypt(HadoopOutputFile.fromPath(
        new org.apache.hadoop.fs.Path(table.location(), deleteFilePath), new Configuration()));

    PartitionKey part = new PartitionKey(table.spec(), eqDeleteRowSchema);
    part.partition(rowsToDelete.get(0));
    EqualityDeleteWriter<Record> eqWriter = appenderFactory.newEqDeleteWriter(outputFile, fileFormat, part);
    try (EqualityDeleteWriter<Record> writer = eqWriter) {
      writer.write(rowsToDelete);
    }
    return eqWriter.toDeleteFile();
  }

  /**
   * @param table The table to create the delete file for
   * @param deleteFilePath The path where the delete file should be created, relative to the table location root
   * @param fileFormat The file format that should be used for writing out the delete file
   * @param partitionValues A map of partition values (partitionKey=partitionVal, ...) to be used for the delete file
   * @param deletes The list of position deletes, each containing the data file path, the position of the row in the
   *                data file and the row itself that should be deleted
   * @return The DeleteFile created
   * @throws IOException If there is an error during DeleteFile write
   */
  public static DeleteFile createPositionalDeleteFile(Table table, String deleteFilePath, FileFormat fileFormat,
      Map<String, Object> partitionValues, List<PositionDelete<Record>> deletes) throws IOException {

    Schema posDeleteRowSchema = deletes.get(0).row() == null ? null : table.schema();
    FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(),
        null, null, posDeleteRowSchema);
    EncryptedOutputFile outputFile = table.encryption().encrypt(HadoopOutputFile.fromPath(
        new org.apache.hadoop.fs.Path(table.location(), deleteFilePath), new Configuration()));

    PartitionKey partitionKey = null;
    if (partitionValues != null) {
      Record record = GenericRecord.create(table.schema()).copy(partitionValues);
      partitionKey = new PartitionKey(table.spec(), table.schema());
      partitionKey.partition(record);
    }

    PositionDeleteWriter<Record> posWriter = appenderFactory.newPosDeleteWriter(outputFile, fileFormat, partitionKey);
    try (PositionDeleteWriter<Record> writer = posWriter) {
      deletes.forEach(del -> {
        PositionDelete positionDelete = PositionDelete.create();
        positionDelete.set(del.path(), del.pos(), del.row());
        writer.write(positionDelete);
      });
    }
    return posWriter.toDeleteFile();
  }

  /**
   * Get the timestamp string which we can use in the queries. The timestamp will be after the given snapshot
   * and before the next one
   * @param table The table which we want to query
   * @param snapshotPosition The position of the last snapshot we want to see in the query results
   * @return The timestamp which we can use in the queries
   */
  public static String timestampAfterSnapshot(Table table, int snapshotPosition) {
    List<HistoryEntry> history = table.history();
    long snapshotTime = history.get(snapshotPosition).timestampMillis();
    long time = snapshotTime + 100;
    if (history.size() > snapshotPosition + 1) {
      time = snapshotTime + ((history.get(snapshotPosition + 1).timestampMillis() - snapshotTime) / 2);
    }

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000000");
    return simpleDateFormat.format(new Date(time));
  }

}
