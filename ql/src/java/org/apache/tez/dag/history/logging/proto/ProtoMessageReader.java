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

package org.apache.tez.dag.history.logging.proto;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.Reader;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public class ProtoMessageReader<T extends MessageLite> implements Closeable {
  private final Path filePath;
  private final Reader reader;
  private final ProtoMessageWritable<T> writable;

  ProtoMessageReader(Configuration conf, Path filePath, Parser<T> parser) throws IOException {
    this.filePath = filePath;
    // The writer does not flush the length during hflush. Using length options lets us read
    // past length in the FileStatus but it will throw EOFException during a read instead
    // of returning null.
    this.reader = new Reader(conf, Reader.file(filePath), Reader.length(Long.MAX_VALUE));
    this.writable = new ProtoMessageWritable<>(parser);
  }

  public Path getFilePath() {
    return filePath;
  }

  public void setOffset(long offset) throws IOException {
    reader.seek(offset);
  }

  public long getOffset() throws IOException {
    return reader.getPosition();
  }

  public T readEvent() throws IOException {
    if (!reader.next(NullWritable.get(), writable)) {
      return null;
    }
    return writable.getMessage();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
