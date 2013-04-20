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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow;
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
                 FileSinkOperator.RecordWriter {
    private Writer writer = null;
    private final FileSystem fs;
    private final Path path;
    private final Configuration conf;
    private final long stripeSize;
    private final int compressionSize;
    private final CompressionKind compress;
    private final int rowIndexStride;

    OrcRecordWriter(FileSystem fs, Path path, Configuration conf,
                    String stripeSize, String compress,
                    String compressionSize, String rowIndexStride) {
      this.fs = fs;
      this.path = path;
      this.conf = conf;
      this.stripeSize = Long.valueOf(stripeSize);
      this.compress = CompressionKind.valueOf(compress);
      this.compressionSize = Integer.valueOf(compressionSize);
      this.rowIndexStride = Integer.valueOf(rowIndexStride);
    }

    @Override
    public void write(NullWritable nullWritable,
                      OrcSerdeRow row) throws IOException {
      if (writer == null) {
        writer = OrcFile.createWriter(fs, path, this.conf, row.getInspector(),
            stripeSize, compress, compressionSize, rowIndexStride);
      }
      writer.addRow(row.getRow());
    }

    @Override
    public void write(Writable row) throws IOException {
      OrcSerdeRow serdeRow = (OrcSerdeRow) row;
      if (writer == null) {
        writer = OrcFile.createWriter(fs, path, this.conf,
            serdeRow.getInspector(), stripeSize, compress, compressionSize,
            rowIndexStride);
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
        writer = OrcFile.createWriter(fs, path, this.conf, inspector,
            stripeSize, compress, compressionSize, rowIndexStride);
      }
      writer.close();
    }
  }

  @Override
  public RecordWriter<NullWritable, OrcSerdeRow>
      getRecordWriter(FileSystem fileSystem, JobConf conf, String name,
                      Progressable reporter) throws IOException {
    return new OrcRecordWriter(fileSystem,  new Path(name), conf,
      OrcFile.DEFAULT_STRIPE_SIZE, OrcFile.DEFAULT_COMPRESSION,
      OrcFile.DEFAULT_COMPRESSION_BLOCK_SIZE, OrcFile.DEFAULT_ROW_INDEX_STRIDE);
  }

  @Override
  public FileSinkOperator.RecordWriter
     getHiveRecordWriter(JobConf conf,
                         Path path,
                         Class<? extends Writable> valueClass,
                         boolean isCompressed,
                         Properties tableProperties,
                         Progressable reporter) throws IOException {
    String stripeSize = tableProperties.getProperty(OrcFile.STRIPE_SIZE,
        OrcFile.DEFAULT_STRIPE_SIZE);
    String compression = tableProperties.getProperty(OrcFile.COMPRESSION,
        OrcFile.DEFAULT_COMPRESSION);
    String compressionSize =
      tableProperties.getProperty(OrcFile.COMPRESSION_BLOCK_SIZE,
        OrcFile.DEFAULT_COMPRESSION_BLOCK_SIZE);
    String rowIndexStride =
        tableProperties.getProperty(OrcFile.ROW_INDEX_STRIDE,
            OrcFile.DEFAULT_ROW_INDEX_STRIDE);
    if ("false".equals(tableProperties.getProperty(OrcFile.ENABLE_INDEXES))) {
      rowIndexStride = "0";
    }
    return new OrcRecordWriter(path.getFileSystem(conf), path, conf,
      stripeSize, compression, compressionSize, rowIndexStride);
  }
}
