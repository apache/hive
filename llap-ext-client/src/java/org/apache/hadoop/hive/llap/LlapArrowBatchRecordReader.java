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

import com.google.common.base.Preconditions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.hive.ql.io.arrow.RootAllocatorFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

/*
 * Read from Arrow stream batch-by-batch
 */
public class LlapArrowBatchRecordReader extends LlapBaseRecordReader<ArrowWrapperWritable> {

  private BufferAllocator allocator;
  private ArrowStreamReader arrowStreamReader;

  public LlapArrowBatchRecordReader(InputStream in, Schema schema, Class<ArrowWrapperWritable> clazz,
      JobConf job, Closeable client, Socket socket, long arrowAllocatorLimit) throws IOException {
    super(in, schema, clazz, job, client, socket);
    allocator = RootAllocatorFactory.INSTANCE.getOrCreateRootAllocator(arrowAllocatorLimit);
    this.arrowStreamReader = new ArrowStreamReader(socket.getInputStream(), allocator);
  }

  @Override
  public boolean next(NullWritable key, ArrowWrapperWritable value) throws IOException {
    try {
      // Need a way to know what thread to interrupt, since this is a blocking thread.
      setReaderThread(Thread.currentThread());

      boolean hasInput = arrowStreamReader.loadNextBatch();
      if (hasInput) {
        VectorSchemaRoot vectorSchemaRoot = arrowStreamReader.getVectorSchemaRoot();
        //There must be at least one column vector
        Preconditions.checkState(vectorSchemaRoot.getFieldVectors().size() > 0);
        if(vectorSchemaRoot.getFieldVectors().get(0).getValueCount() == 0) {
          //An empty batch will appear at the end of the stream
          return false;
        }
        value.setVectorSchemaRoot(arrowStreamReader.getVectorSchemaRoot());
        return true;
      } else {
        processReaderEvent();
        return false;
      }
    } catch (IOException io) {
      failOnInterruption(io);
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    arrowStreamReader.close();
  }

}

