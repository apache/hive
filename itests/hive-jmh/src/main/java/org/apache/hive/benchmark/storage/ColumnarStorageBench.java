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

package org.apache.hive.benchmark.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.util.Arrays;

@State(Scope.Benchmark)
public class ColumnarStorageBench {
 /**
  * This test measures the performance between different columnar storage formats used
  * by Hive. If you need to add more formats, see the 'format' gobal variable to add
  * a new one on the list, and create a class that implements StorageFormatTest interface.
  *
  * This test uses JMH framework for benchmarking.
  * You may execute this benchmark tool using JMH command line in different ways:
  *
  * To use the settings shown in the main() function, use:
  * $ java -cp target/benchmarks.jar org.apache.hive.benchmark.storage.ColumnarStorageBench
  *
  * To use the default settings used by JMH, use:
  * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.storage ColumnStorageBench
  *
  * To specify different parameters, use:
  * - This command will use 10 warm-up iterations, 5 test iterations, and 2 forks. And it will
  *   display the Average Time (avgt) in Microseconds (us)
  * - Benchmark mode. Available modes are:
  *   [Throughput/thrpt, AverageTime/avgt, SampleTime/sample, SingleShotTime/ss, All/all]
  * - Output time unit. Available time units are: [m, s, ms, us, ns].
  *
  * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.storage ColumnStorageBench -wi 10 -i 5 -f 2 -bm avgt -tu us
  */

  private static final String DEFAULT_TEMP_LOCATION = "/tmp";

  private File writeFile, readFile, recordWriterFile;
  private Path writePath, readPath, recordWriterPath;
  private FileSystem fs;

  /**
   * Contains implementation for the storage format to test
   */
  private StorageFormatTest storageFormatTest;

  private RecordWriter recordWriter;
  private RecordReader recordReader;

  /**
   * These objects contains the record to be tested.
   */
  private Writable recordWritable[];
  private Object rows[];
  private StructObjectInspector oi;

  /**
   * These column types are used for the record that will be tested.
   */
  private Properties recordProperties;
  private String DEFAULT_COLUMN_TYPES = "int,double,boolean,string,array<int>,map<string,string>,struct<a:int,b:int>";

  public ColumnarStorageBench() {
    recordProperties = new Properties();
    recordProperties.setProperty("columns", getColumnNames(DEFAULT_COLUMN_TYPES));
    recordProperties.setProperty("columns.types", DEFAULT_COLUMN_TYPES);

    oi = getObjectInspector(DEFAULT_COLUMN_TYPES);

    final int NUMBER_OF_ROWS_TO_TEST = 100;
    rows = new Object[NUMBER_OF_ROWS_TO_TEST];
    recordWritable = new Writable[NUMBER_OF_ROWS_TO_TEST];

    for (int i=0; i<NUMBER_OF_ROWS_TO_TEST; i++) {
      rows[i] = createRandomRow(DEFAULT_COLUMN_TYPES);
    }
  }

  private String getColumnNames(final String columnTypes) {
    StringBuilder columnNames = new StringBuilder();

    /* Construct a string of column names based on the number of column types */
    List<TypeInfo> columnTypesList = TypeInfoUtils.getTypeInfosFromTypeString(columnTypes);
    for (int i=0; i < columnTypesList.size(); i++) {
      if (i > 0) {
        columnNames.append(",");
      }
      columnNames.append("c" + i);
    }

    return columnNames.toString();
  }

  private long fileLength(Path path) throws IOException {
    return fs.getFileStatus(path).getLen();
  }

  private ArrayWritable record(Writable... fields) {
    return new ArrayWritable(Writable.class, fields);
  }

  private Writable getPrimitiveWritable(final PrimitiveTypeInfo typeInfo) {
    Random rand = new Random();

    switch (typeInfo.getPrimitiveCategory()) {
      case INT:
        return new IntWritable(rand.nextInt());
      case DOUBLE:
        return new DoubleWritable(rand.nextDouble());
      case BOOLEAN:
        return new BooleanWritable(rand.nextBoolean());
      case CHAR:
      case VARCHAR:
      case STRING:
        byte b[] = new byte[30];
        rand.nextBytes(b);
        return new BytesWritable(b);
      default:
        throw new IllegalArgumentException("Invalid primitive type: " + typeInfo.getTypeName());
    }
  }

  private ArrayWritable createRecord(final List<TypeInfo> columnTypes) {
    Writable[] fields = new Writable[columnTypes.size()];

    int pos=0;
    for (TypeInfo type : columnTypes) {
      switch (type.getCategory()) {
        case PRIMITIVE:
          fields[pos++] = getPrimitiveWritable((PrimitiveTypeInfo)type);
        break;
        case LIST: {
          List<TypeInfo> elementType = new ArrayList<TypeInfo>();
          elementType.add(((ListTypeInfo) type).getListElementTypeInfo());
          fields[pos++] = record(createRecord(elementType));
        } break;
        case MAP: {
          List<TypeInfo> keyValueType = new ArrayList<TypeInfo>();
          keyValueType.add(((MapTypeInfo) type).getMapKeyTypeInfo());
          keyValueType.add(((MapTypeInfo) type).getMapValueTypeInfo());
          fields[pos++] = record(record(createRecord(keyValueType)));
        } break;
        case STRUCT: {
          List<TypeInfo> elementType = ((StructTypeInfo) type).getAllStructFieldTypeInfos();
          fields[pos++] = createRecord(elementType);
        } break;
        default:
          throw new IllegalStateException("Invalid column type: " + type);
      }
    }

    return record(fields);
  }

  private StructObjectInspector getObjectInspector(final String columnTypes) {
    List<TypeInfo> columnTypeList = TypeInfoUtils.getTypeInfosFromTypeString(columnTypes);
    List<String> columnNameList = Arrays.asList(getColumnNames(columnTypes).split(","));
    StructTypeInfo rowTypeInfo = (StructTypeInfo)TypeInfoFactory.getStructTypeInfo(columnNameList, columnTypeList);

    return new ArrayWritableObjectInspector(rowTypeInfo);
  }

  private Object createRandomRow(final String columnTypes) {
    return createRecord(TypeInfoUtils.getTypeInfosFromTypeString(columnTypes));
  }

  /**
   * This class encapsulates all methods that will be called by each of the @Benchmark
   * methods.
   */
  private class StorageFormatTest {
    private SerDe serDe;
    private JobConf jobConf;
    private HiveOutputFormat outputFormat;
    private InputFormat inputFormat;

    public StorageFormatTest(SerDe serDeImpl, HiveOutputFormat outputFormatImpl, InputFormat inputFormatImpl) throws SerDeException {
      jobConf = new JobConf();
      serDe = serDeImpl;
      outputFormat = outputFormatImpl;
      inputFormat = inputFormatImpl;

      Configuration conf = new Configuration();
      SerDeUtils.initializeSerDe(serDe, conf, recordProperties, null);
    }

    public Writable serialize(Object row, StructObjectInspector oi) throws SerDeException {
      return serDe.serialize(row, oi);
    }

    public Object deserialize(Writable record) throws SerDeException {
      return serDe.deserialize(record);
    }

    /* We write many records because sometimes the RecordWriter for the format to test
     * behaves different with one record than a bunch of records */
    public void writeRecords(RecordWriter writer, Writable records[]) throws IOException {
      for (int i=0; i < records.length; i++) {
        writer.write(records[i]);
      }
    }

    /* We read many records because sometimes the RecordReader for the format to test
     * behaves different with one record than a bunch of records */
    public Object readRecords(RecordReader reader) throws IOException {
      Object alwaysNull = reader.createKey();
      Object record = reader.createValue();

      // Just loop through all values. We do not need to store anything though.
      // This is just for test purposes
      while (reader.next(alwaysNull, record)) ;

      return record;
    }

    public RecordWriter getRecordWriter(Path outputPath) throws IOException {
      return outputFormat.getHiveRecordWriter(jobConf, outputPath, null, false, recordProperties, null);
    }

    public RecordReader getRecordReader(Path inputPath) throws IOException {
      return inputFormat.getRecordReader(
          new FileSplit(inputPath, 0, fileLength(inputPath), (String[]) null),
          jobConf, null);
    }
  }

  /**
   * This class is called to run I/O parquet tests.
   */
  private class ParquetStorageFormatTest extends StorageFormatTest {
    public ParquetStorageFormatTest() throws SerDeException {
      super(new ParquetHiveSerDe(), new MapredParquetOutputFormat(), new MapredParquetInputFormat());
    }
  }

  /**
   * This class is called to run i/o orc tests.
   */
  private class OrcStorageFormatTest extends StorageFormatTest {
    public OrcStorageFormatTest() throws SerDeException {
      super(new OrcSerde(), new OrcOutputFormat(), new OrcInputFormat());
    }
  }

  private File createTempFile() throws IOException {
    if (URI.create(DEFAULT_TEMP_LOCATION).getScheme() != null) {
      throw new IOException("Cannot create temporary files in a non-local file-system: Operation not permitted.");
    }

    File temp = File.createTempFile(this.toString(), null, new File(DEFAULT_TEMP_LOCATION));
    temp.deleteOnExit();
    temp.delete();

    return temp;
  }

  // Test different format types
  @Param({"orc", "parquet"})
  public String format;

  /**
   * Initializes resources that will be needed for each of the benchmark tests.
   *
   * @throws SerDeException If it cannot initialize the desired test format.
   * @throws IOException If it cannot write data to temporary files.
   */
  @Setup(Level.Trial)
  public void prepareBenchmark() throws SerDeException, IOException {
    if (format.equalsIgnoreCase("parquet")) {
      storageFormatTest = new ParquetStorageFormatTest();
    } else if (format.equalsIgnoreCase("orc")) {
      storageFormatTest = new OrcStorageFormatTest();
    } else {
      throw new IllegalArgumentException("Invalid file format argument: " + format);
    }

    for (int i=0; i < rows.length; i++) {
      recordWritable[i] = storageFormatTest.serialize(rows[i], oi);
    }

    fs = FileSystem.getLocal(new Configuration());

    writeFile = createTempFile();
    writePath = new Path(writeFile.getPath());

    readFile = createTempFile();
    readPath = new Path(readFile.getPath());

    /*
     * Write a bunch of random rows that will be used for read benchmark.
     */
    RecordWriter writer = storageFormatTest.getRecordWriter(readPath);
    storageFormatTest.writeRecords(writer, recordWritable);
    writer.close(false);
  }

  /**
   * It deletes any temporary file created by prepareBenchmark.
   */
  @TearDown(Level.Trial)
  public void cleanUpBenchmark() {
    readFile.delete();
  }

  /**
   * This method is invoked before every call to the methods to test. It creates
   * resources that are needed for each call (not in a benchmark level).
   *
   * @throws IOException If it cannot writes temporary files.
   */
  @Setup(Level.Invocation)
  public void prepareInvocation() throws IOException {
    recordWriterFile = createTempFile();
    recordWriterPath = new Path(recordWriterFile.getPath());

    recordWriter = storageFormatTest.getRecordWriter(writePath);
    recordReader = storageFormatTest.getRecordReader(readPath);
  }

  /**
   * This method is invoked after every call to the methods to test. It closes
   * and cleans up all temporary files.
   *
   * @throws IOException If it cannot close or delete temporary files.
   */
  @TearDown(Level.Invocation)
  public void cleanUpInvocation() throws IOException {
    recordWriter.close(false);
    recordReader.close();

    recordWriterFile.delete();
    writeFile.delete();
  }

  @Benchmark
  public void write() throws IOException {
    storageFormatTest.writeRecords(recordWriter, recordWritable);
  }

  @Benchmark
  public Object read() throws IOException {
    return storageFormatTest.readRecords(recordReader);
  }

  @Benchmark
  public Writable serialize() throws SerDeException {
    return storageFormatTest.serialize(rows[0], oi);
  }

  @Benchmark
  public Object deserialize() throws SerDeException {
    return storageFormatTest.deserialize(recordWritable[0]);
  }

  @Benchmark
  public RecordWriter getRecordWriter() throws IOException {
    return storageFormatTest.getRecordWriter(recordWriterPath);
  }

  @Benchmark
  public RecordReader getRecordReader() throws IOException {
    return storageFormatTest.getRecordReader(readPath);
  }

  public static void main(String args[]) throws Exception {
    Options opt = new OptionsBuilder()
        .include(ColumnarStorageBench.class.getSimpleName())
        .warmupIterations(1)
        .measurementIterations(1)
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
