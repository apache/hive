/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.hooks;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public class ProtoMessageWriter<T extends MessageLite> implements Closeable {
  private final Path filePath;
  private final SequenceFile.Writer writer;
  private final ProtoMessageWritable<T> writable;

  ProtoMessageWriter(Configuration conf, Path filePath, Parser<T> parser) throws IOException {
    this.filePath = filePath;
    this.writer = SequenceFile.createWriter(
        conf,
        SequenceFile.Writer.file(filePath),
        SequenceFile.Writer.keyClass(NullWritable.class),
        SequenceFile.Writer.valueClass(ProtoMessageWritable.class),
        SequenceFile.Writer.appendIfExists(true),
        SequenceFile.Writer.compression(CompressionType.RECORD));
    this.writable = new ProtoMessageWritable<>(parser);
  }

  public Path getPath() {
    return filePath;
  }

  public long getOffset() throws IOException {
    return writer.getLength();
  }

  public void writeProto(T message) throws IOException {
    writable.setMessage(message);
    writer.append(NullWritable.get(), writable);
  }

  public void hflush() throws IOException {
    writer.hflush();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
