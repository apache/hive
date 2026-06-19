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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.anon.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public final class FileUtils {

  public final static int ORC_BUFFER_SIZE = 2 * 256 * 1024;
  public final static int ORC_STRIP_SIZE = 2 * 64 * 1024 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  public static Writer getWriter(Path path, Configuration conf, ObjectInspector inspector) {

    try {
      return OrcFile.createWriter(path, OrcFile.writerOptions(conf).
        inspector(inspector).
        stripeSize(ORC_STRIP_SIZE).
        bufferSize(ORC_BUFFER_SIZE)
      );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Reader getRecordReader(Path path, Configuration conf) throws IOException {
    return OrcFile.createReader(path, OrcFile.readerOptions(conf));
  }

  public static boolean createFile(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return fs.createNewFile(path);
  }

  public static boolean fileExists(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return fs.exists(path);
  }

  public static boolean deleteFile(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return fs.delete(path, false);
  }

  public static void deleteIfExists(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
  }

  public static RecordReader<NullWritable, ArrayWritable> getReader(Path path, Configuration conf) throws IOException {
    JobConf jobConf = new JobConf(conf);
    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);
    long len = status.getLen();
    InputSplit split = new FileSplit(path, 0L, len, (String[]) null);

    return new MapredParquetInputFormat().getRecordReader(split, jobConf, null);

  }

  public static FileSinkOperator.RecordWriter getWriter(Path path, Configuration conf) throws IOException {
    JobConf jobConf = new JobConf(conf);


    final String columnNameProperty = conf.get(IOConstants.COLUMNS);
    final String columnTypeProperty = conf.get(IOConstants.COLUMNS_TYPES);

    List<String> columnNames = Collections.emptyList();
    List<TypeInfo> columnTypes = Collections.emptyList();
    final String columnNameDelimiter = String.valueOf(SerDeUtils.COMMA);

    if (!columnNameProperty.isEmpty()) {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    }

    if (!columnTypeProperty.isEmpty()) {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    Properties tableProperties = new Properties();
    tableProperties.setProperty(IOConstants.COLUMNS, columnNameProperty);
    tableProperties.setProperty(IOConstants.COLUMNS_TYPES, columnTypeProperty);

    MessageType messageType = HiveSchemaConverter.convert(columnNames, columnTypes, jobConf);
    DataWritableWriteSupport.setSchema(messageType, jobConf);

    MapredParquetOutputFormat outputFormat = new MapredParquetOutputFormat();

    return outputFormat.getHiveRecordWriter(jobConf, path, ParquetHiveRecord.class, false, tableProperties, null);
  }

  public static StructObjectInspector getOI(final Configuration conf) {
    final String columnNameProperty = conf.get(IOConstants.COLUMNS);
    final String columnTypeProperty = conf.get(IOConstants.COLUMNS_TYPES);
    List<String> columnNames = Collections.emptyList();
    List<TypeInfo> columnTypes = Collections.emptyList();
    final String columnNameDelimiter = String.valueOf(SerDeUtils.COMMA);

    if (!columnNameProperty.isEmpty()) {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    }
    if (!columnTypeProperty.isEmpty()) {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }
    StructTypeInfo completeTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    StructObjectInspector soi = new ArrayWritableObjectInspector(completeTypeInfo, null);

    return soi;
  }
}
