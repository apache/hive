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

package org.apache.hadoop.hive.ql.io.parquet;

import com.google.common.base.Joiner;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

public abstract class AbstractTestParquetDirect {

  public static FileSystem localFS = null;

  @BeforeClass
  public static void initializeFS() throws IOException {
    localFS = FileSystem.getLocal(new Configuration());
  }

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();


  public interface DirectWriter {
    public void write(RecordConsumer consumer);
  }

  public static class DirectWriteSupport extends WriteSupport<Void> {
    private RecordConsumer recordConsumer;
    private final MessageType type;
    private final DirectWriter writer;
    private final Map<String, String> metadata;

    private DirectWriteSupport(MessageType type, DirectWriter writer,
                               Map<String, String> metadata) {
      this.type = type;
      this.writer = writer;
      this.metadata = metadata;
    }

    @Override
    public WriteContext init(Configuration configuration) {
      return new WriteContext(type, metadata);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Void record) {
      writer.write(recordConsumer);
    }
  }

  public Path writeDirect(String name, MessageType type, DirectWriter writer)
      throws IOException {
    File temp = tempDir.newFile(name + ".parquet");
    temp.deleteOnExit();
    temp.delete();

    Path path = new Path(temp.getPath());

    ParquetWriter<Void> parquetWriter = new ParquetWriter<Void>(path,
        new DirectWriteSupport(type, writer, new HashMap<String, String>()));
    parquetWriter.write(null);
    parquetWriter.close();

    return path;
  }

  public static ArrayWritable record(Writable... fields) {
    return new ArrayWritable(Writable.class, fields);
  }

  public static ArrayWritable list(Writable... elements) {
    // the ObjectInspector for array<?> and map<?, ?> expects an extra layer
    return new ArrayWritable(ArrayWritable.class, new ArrayWritable[] {
        new ArrayWritable(Writable.class, elements)
    });
  }

  public static String toString(ArrayWritable arrayWritable) {
    Writable[] writables = arrayWritable.get();
    String[] strings = new String[writables.length];
    for (int i = 0; i < writables.length; i += 1) {
      if (writables[i] instanceof ArrayWritable) {
        strings[i] = toString((ArrayWritable) writables[i]);
      } else {
        strings[i] = String.valueOf(writables[i]);
      }
    }
    return Arrays.toString(strings);
  }

  public static void assertEquals(String message, ArrayWritable expected,
                                  ArrayWritable actual) {
    Assert.assertEquals(message, toString(expected), toString(actual));
  }

  public static List<ArrayWritable> read(Path parquetFile) throws IOException {
    List<ArrayWritable> records = new ArrayList<ArrayWritable>();

    RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().
        getRecordReader(new FileSplit(
                parquetFile, 0, fileLength(parquetFile), (String[]) null),
            new JobConf(), null);

    NullWritable alwaysNull = reader.createKey();
    ArrayWritable record = reader.createValue();
    while (reader.next(alwaysNull, record)) {
      records.add(record);
      record = reader.createValue(); // a new value so the last isn't clobbered
    }

    return records;
  }

  public static long fileLength(Path localFile) throws IOException {
    return localFS.getFileStatus(localFile).getLen();
  }

  private static final Joiner COMMA = Joiner.on(",");
  public void deserialize(Writable record, List<String> columnNames,
                          List<String> columnTypes) throws Exception {
    ParquetHiveSerDe serde = new ParquetHiveSerDe();
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, COMMA.join(columnNames));
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, COMMA.join(columnTypes));
    serde.initialize(null, props);
    serde.deserialize(record);
  }
}
