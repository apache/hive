/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.avro;

import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;
import static org.apache.avro.mapred.AvroJob.OUTPUT_CODEC;
import static org.apache.avro.mapred.AvroOutputFormat.DEFAULT_DEFLATE_LEVEL;
import static org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * Write to an Avro file from a Hive process.
 */
public class AvroContainerOutputFormat
        implements HiveOutputFormat<LongWritable, AvroGenericRecordWritable> {

  @Override
  public FSRecordWriter getHiveRecordWriter(JobConf jobConf,
         Path path, Class<? extends Writable> valueClass, boolean isCompressed,
         Properties properties, Progressable progressable) throws IOException {
    Schema schema;
    try {
      schema = AvroSerdeUtils.determineSchemaOrThrowException(properties);
    } catch (AvroSerdeException e) {
      throw new IOException(e);
    }
    GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dfw = new DataFileWriter<GenericRecord>(gdw);

    if (isCompressed) {
      int level = jobConf.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
      String codecName = jobConf.get(OUTPUT_CODEC, DEFLATE_CODEC);
      CodecFactory factory = codecName.equals(DEFLATE_CODEC)
          ? CodecFactory.deflateCodec(level)
          : CodecFactory.fromString(codecName);
      dfw.setCodec(factory);
    }

    dfw.create(schema, path.getFileSystem(jobConf).create(path));
    return new AvroGenericRecordWriter(dfw);
  }

    //no records will be emitted from Hive
  @Override
  public RecordWriter<LongWritable, AvroGenericRecordWritable>
  getRecordWriter(FileSystem ignored, JobConf job, String name,
      Progressable progress) {
    return new RecordWriter<LongWritable, AvroGenericRecordWritable>() {
      public void write(LongWritable key, AvroGenericRecordWritable value) {
        throw new RuntimeException("Should not be called");
      }

      public void close(Reporter reporter) {
      }
    };
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    return; // Not doing any check
  }
}
