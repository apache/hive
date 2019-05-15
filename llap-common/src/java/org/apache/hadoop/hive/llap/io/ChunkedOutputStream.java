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

package org.apache.hadoop.hive.llap.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Writes data out as a series of chunks in the form <chunk size><chunk bytes><chunk size><chunk bytes>
// Closing the output stream will send a final 0-length chunk which will indicate end of input.
public class ChunkedOutputStream extends OutputStream {

  static final private Logger LOG = LoggerFactory.getLogger(ChunkedOutputStream.class);

  private DataOutputStream dout;
  private byte[] singleByte = new byte[1];
  private byte[] buffer;
  private int bufPos = 0;
  private String id;

  public ChunkedOutputStream(OutputStream out, int bufSize, String id) {
    LOG.debug("Creating chunked input stream: {}", id);
    if (bufSize <= 0) {
      throw new IllegalArgumentException("Positive bufSize required, was " + bufSize);
    }
    buffer = new byte[bufSize];
    dout = new DataOutputStream(out);
    this.id = id;
  }

  @Override
  public void write(int b) throws IOException {
    singleByte[0] = (byte) b;
    write(singleByte, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    int bytesWritten = 0;
    while (bytesWritten < len) {
      // Copy the data to the buffer
      int bytesToWrite = Math.min(len - bytesWritten, buffer.length - bufPos);
      System.arraycopy(b, off + bytesWritten, buffer, bufPos, bytesToWrite);
      bytesWritten += bytesToWrite;
      bufPos += bytesToWrite;

      // If we've filled the buffer, write it out
      if (bufPos == buffer.length) {
        writeChunk();
      }
    }
  }

  @Override
  public void close() throws IOException {
    flush();

    // Write final 0-length chunk
    writeChunk();

    LOG.debug("{}: Closing underlying output stream.", id);
    dout.close();
  }

  @Override
  public void flush() throws IOException {
    // Write any remaining bytes to the out stream.
    if (bufPos > 0) {
      writeChunk();
      dout.flush();
    }
  }

  private void writeChunk() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: Writing chunk of size {}", id, bufPos);
    }

    // First write chunk length
    dout.writeInt(bufPos);

    // Then write chunk bytes
    dout.write(buffer, 0, bufPos);

    bufPos = 0; // reset buffer
  }
}
