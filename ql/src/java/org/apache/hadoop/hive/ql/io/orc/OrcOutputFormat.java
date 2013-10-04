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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * A Hive OutputFormat for ORC files.
 */
public class OrcOutputFormat extends FileOutputFormat<NullWritable, OrcSerdeRow>
                        implements HiveOutputFormat<NullWritable, OrcSerdeRow> {

  private static class OrcRecordWriter
      implements RecordWriter<NullWritable, OrcSerdeRow>,
                 FSRecordWriter,
                 FSRecordWriter.StatsProvidingRecordWriter {
    private Writer writer = null;
    private final Path path;
    private final OrcFile.WriterOptions options;
    private final SerDeStats stats;

    OrcRecordWriter(Path path, OrcFile.WriterOptions options) {
      this.path = path;
      this.options = options;
      this.stats = new SerDeStats();
    }

    @Override
    public void write(NullWritable nullWritable,
                      OrcSerdeRow row) throws IOException {
      if (writer == null) {
        options.inspector(row.getInspector());
        writer = OrcFile.createWriter(path, options);
      }
      writer.addRow(row.getRow());
    }

    @Override
    public void write(Writable row) throws IOException {
      OrcSerdeRow serdeRow = (OrcSerdeRow) row;
      if (writer == null) {
        options.inspector(serdeRow.getInspector());
        writer = OrcFile.createWriter(path, options);
      }
      writer.addRow(serdeRow.getRow());
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      close(true);
    }

    @Override
    public void close(boolean b) throws IOException {
      // if we haven't written any rows, we need to create a file with a
      // generic schema.
      if (writer == null) {
        // a row with no columns
        ObjectInspector inspector = ObjectInspectorFactory.
            getStandardStructObjectInspector(new ArrayList<String>(),
                new ArrayList<ObjectInspector>());
        options.inspector(inspector);
        writer = OrcFile.createWriter(path, options);
      }
      writer.close();
    }

    @Override
    public SerDeStats getStats() {
      stats.setRawDataSize(writer.getRawDataSize());
      stats.setRowCount(writer.getNumberOfRows());
      return stats;
    }
  }

  @Override
  public RecordWriter<NullWritable, OrcSerdeRow>
      getRecordWriter(FileSystem fileSystem, JobConf conf, String name,
                      Progressable reporter) throws IOException {
    return new
      OrcRecordWriter(new Path(name), OrcFile.writerOptions(conf));
  }

  @Override
  public FSRecordWriter
     getHiveRecordWriter(JobConf conf,
                         Path path,
                         Class<? extends Writable> valueClass,
                         boolean isCompressed,
                         Properties tableProperties,
                         Progressable reporter) throws IOException {
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf);
    if (tableProperties.containsKey(OrcFile.STRIPE_SIZE)) {
      options.stripeSize(Long.parseLong
                           (tableProperties.getProperty(OrcFile.STRIPE_SIZE)));
    }

    if (tableProperties.containsKey(OrcFile.COMPRESSION)) {
      options.compress(CompressionKind.valueOf
                           (tableProperties.getProperty(OrcFile.COMPRESSION)));
    }

    if (tableProperties.containsKey(OrcFile.COMPRESSION_BLOCK_SIZE)) {
      options.bufferSize(Integer.parseInt
                         (tableProperties.getProperty
                            (OrcFile.COMPRESSION_BLOCK_SIZE)));
    }

    if (tableProperties.containsKey(OrcFile.ROW_INDEX_STRIDE)) {
      options.rowIndexStride(Integer.parseInt
                             (tableProperties.getProperty
                              (OrcFile.ROW_INDEX_STRIDE)));
    }

    if (tableProperties.containsKey(OrcFile.ENABLE_INDEXES)) {
      if ("false".equals(tableProperties.getProperty
                         (OrcFile.ENABLE_INDEXES))) {
        options.rowIndexStride(0);
      }
    }

    if (tableProperties.containsKey(OrcFile.BLOCK_PADDING)) {
      options.blockPadding(Boolean.parseBoolean
                           (tableProperties.getProperty
                            (OrcFile.BLOCK_PADDING)));
    }

    return new OrcRecordWriter(path, options);
  }
}
