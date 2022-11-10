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

package org.apache.hadoop.hive.llap;

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes Arrow batches to an {@link org.apache.arrow.vector.ipc.ArrowStreamWriter}.
 * The byte stream will be formatted according to the Arrow Streaming format.
 * Because ArrowStreamWriter is bound to a {@link org.apache.arrow.vector.VectorSchemaRoot}
 * when it is created,
 * calls to the {@link #write(Writable, Writable)} method only serve as a signal that
 * a new batch has been loaded to the associated VectorSchemaRoot.
 * Payload data for writing is indirectly made available by reference:
 * ArrowStreamWriter -&gt; VectorSchemaRoot -&gt; List&lt;FieldVector&gt;
 * i.e. both they key and value are ignored once a reference to the VectorSchemaRoot
 * is obtained.
 */
public class LlapArrowRecordWriter<K extends Writable, V extends Writable>
    implements RecordWriter<K, V> {
  public static final Logger LOG = LoggerFactory.getLogger(LlapArrowRecordWriter.class);

  ArrowStreamWriter arrowStreamWriter;
  VectorSchemaRoot vectorSchemaRoot;
  WritableByteChannelAdapter out;
  BufferAllocator allocator;
  NonNullableStructVector rootVector;

  public LlapArrowRecordWriter(WritableByteChannelAdapter out) {
    this.out = out;
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    try {
      arrowStreamWriter.close();
    } finally {
      rootVector.close();
      //bytesLeaked should always be 0
      long bytesLeaked = allocator.getAllocatedMemory();
      if(bytesLeaked != 0) {
        LOG.error("Arrow memory leaked bytes: {}", bytesLeaked);
        throw new IllegalStateException("Arrow memory leaked bytes:" + bytesLeaked);
      }
      allocator.close();
    }
  }

  @Override
  public void write(K key, V value) throws IOException {
    ArrowWrapperWritable arrowWrapperWritable = (ArrowWrapperWritable) value;
    if (arrowStreamWriter == null) {
      vectorSchemaRoot = arrowWrapperWritable.getVectorSchemaRoot();
      arrowStreamWriter = new ArrowStreamWriter(vectorSchemaRoot, null, out);
      allocator = arrowWrapperWritable.getAllocator();
      this.out.setAllocator(allocator);
      rootVector = arrowWrapperWritable.getRootVector();
    } else {
      // We need to set the row count for the current vector
      // since root is reused by the stream writer.
      vectorSchemaRoot.setRowCount(rootVector.getValueCount());
    }
    arrowStreamWriter.writeBatch();
  }
}
