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
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordWriter;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.EncodingStrategy;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Properties;

/**
 * A Hive OutputFormat for ORC files.
 */
public class OrcOutputFormat extends FileOutputFormat<NullWritable, OrcSerdeRow>
                        implements AcidOutputFormat<NullWritable, OrcSerdeRow> {

  private static class OrcRecordWriter
      implements RecordWriter<NullWritable, OrcSerdeRow>,
                 StatsProvidingRecordWriter {
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

  /**
   * Helper method to get a parameter first from props if present, falling back to JobConf if not.
   * Returns null if key is present in neither.
   */
  private String getSettingFromPropsFallingBackToConf(String key, Properties props, JobConf conf){
    if ((props != null) && props.containsKey(key)){
      return props.getProperty(key);
    } else if(conf != null) {
      // If conf is not null, and the key is not present, Configuration.get() will
      // return null for us. So, we don't have to check if it contains it.
      return conf.get(key);
    } else {
      return null;
    }
  }

  private OrcFile.WriterOptions getOptions(JobConf conf, Properties props) {
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf);
    String propVal ;
    if ((propVal = getSettingFromPropsFallingBackToConf(
        OrcFile.OrcTableProperties.STRIPE_SIZE.getPropName(),props,conf)) != null){
      options.stripeSize(Long.parseLong(propVal));
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(
        OrcFile.OrcTableProperties.COMPRESSION.getPropName(),props,conf)) != null){
      options.compress(CompressionKind.valueOf(propVal));
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(
        OrcFile.OrcTableProperties.COMPRESSION_BLOCK_SIZE.getPropName(),props,conf)) != null){
      options.bufferSize(Integer.parseInt(propVal));
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(
        OrcFile.OrcTableProperties.ROW_INDEX_STRIDE.getPropName(),props,conf)) != null){
      options.rowIndexStride(Integer.parseInt(propVal));
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(
        OrcFile.OrcTableProperties.ENABLE_INDEXES.getPropName(),props,conf)) != null){
      if ("false".equalsIgnoreCase(propVal)) {
        options.rowIndexStride(0);
      }
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(
        OrcFile.OrcTableProperties.BLOCK_PADDING.getPropName(),props,conf)) != null){
      options.blockPadding(Boolean.parseBoolean(propVal));
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(
        OrcFile.OrcTableProperties.ENCODING_STRATEGY.getPropName(),props,conf)) != null){
      options.encodingStrategy(EncodingStrategy.valueOf(propVal));
    }

    return options;
  }

  @Override
  public RecordWriter<NullWritable, OrcSerdeRow>
  getRecordWriter(FileSystem fileSystem, JobConf conf, String name,
                  Progressable reporter) throws IOException {
    return new
        OrcRecordWriter(new Path(name), getOptions(conf,null));
  }


  @Override
  public StatsProvidingRecordWriter
     getHiveRecordWriter(JobConf conf,
                         Path path,
                         Class<? extends Writable> valueClass,
                         boolean isCompressed,
                         Properties tableProperties,
                         Progressable reporter) throws IOException {
    return new OrcRecordWriter(path, getOptions(conf,tableProperties));
  }

  private class DummyOrcRecordUpdater implements RecordUpdater {
    private final Path path;
    private final ObjectInspector inspector;
    private final PrintStream out;

    private DummyOrcRecordUpdater(Path path, Options options) {
      this.path = path;
      this.inspector = options.getInspector();
      this.out = options.getDummyStream();
    }

    @Override
    public void insert(long currentTransaction, Object row) throws IOException {
      out.println("insert " + path + " currTxn: " + currentTransaction +
          " obj: " + stringifyObject(row, inspector));
    }

    @Override
    public void update(long currentTransaction, Object row) throws IOException {
      out.println("update " + path + " currTxn: " + currentTransaction +
          " obj: " + stringifyObject(row, inspector));
    }

    @Override
    public void delete(long currentTransaction, Object row) throws IOException {
      out.println("delete " + path + " currTxn: " + currentTransaction + " obj: " + row);
    }

    @Override
    public void flush() throws IOException {
      out.println("flush " + path);
    }

    @Override
    public void close(boolean abort) throws IOException {
      out.println("close " + path);
    }

    @Override
    public SerDeStats getStats() {
      return null;
    }

    private void stringifyObject(StringBuilder buffer,
                                 Object obj,
                                 ObjectInspector inspector
                                ) throws IOException {
      if (inspector instanceof StructObjectInspector) {
        buffer.append("{ ");
        StructObjectInspector soi = (StructObjectInspector) inspector;
        boolean isFirst = true;
        for(StructField field: soi.getAllStructFieldRefs()) {
          if (isFirst) {
            isFirst = false;
          } else {
            buffer.append(", ");
          }
          buffer.append(field.getFieldName());
          buffer.append(": ");
          stringifyObject(buffer, soi.getStructFieldData(obj, field),
              field.getFieldObjectInspector());
        }
        buffer.append(" }");
      } else if (inspector instanceof PrimitiveObjectInspector) {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) inspector;
        buffer.append(poi.getPrimitiveJavaObject(obj).toString());
      } else {
        buffer.append("*unknown*");
      }
    }

    private String stringifyObject(Object obj,
                                   ObjectInspector inspector
                                  ) throws IOException {
      StringBuilder buffer = new StringBuilder();
      stringifyObject(buffer, obj, inspector);
      return buffer.toString();
    }
  }

  @Override
  public RecordUpdater getRecordUpdater(Path path,
                                        Options options) throws IOException {
    if (options.getDummyStream() != null) {
      return new DummyOrcRecordUpdater(path, options);
    } else {
      return new OrcRecordUpdater(path, options);
    }
  }

  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getRawRecordWriter(Path path,
                                           Options options) throws IOException {
    final Path filename = AcidUtils.createFilename(path, options);
    final OrcFile.WriterOptions opts =
        OrcFile.writerOptions(options.getConfiguration());
    if (!options.isWritingBase()) {
      opts.bufferSize(OrcRecordUpdater.DELTA_BUFFER_SIZE)
          .stripeSize(OrcRecordUpdater.DELTA_STRIPE_SIZE)
          .blockPadding(false)
          .compress(CompressionKind.NONE)
          .rowIndexStride(0);
    }
    final OrcRecordUpdater.KeyIndexBuilder watcher =
        new OrcRecordUpdater.KeyIndexBuilder();
    opts.inspector(options.getInspector())
        .callback(watcher);
    final Writer writer = OrcFile.createWriter(filename, opts);
    return new org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter() {
      @Override
      public void write(Writable w) throws IOException {
        OrcStruct orc = (OrcStruct) w;
        watcher.addKey(
            ((IntWritable) orc.getFieldValue(OrcRecordUpdater.OPERATION)).get(),
            ((LongWritable)
                orc.getFieldValue(OrcRecordUpdater.ORIGINAL_TRANSACTION)).get(),
            ((IntWritable) orc.getFieldValue(OrcRecordUpdater.BUCKET)).get(),
            ((LongWritable) orc.getFieldValue(OrcRecordUpdater.ROW_ID)).get());
        writer.addRow(w);
      }

      @Override
      public void close(boolean abort) throws IOException {
        writer.close();
      }
    };
  }
}
