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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Data is expected to be a series of data chunks in the form <chunk size><chunk bytes><chunk size><chunk bytes>
// The final data chunk should be a 0-length chunk which will indicate end of input.
public class ChunkedInputStream extends InputStream {

  static final private Logger LOG = LoggerFactory.getLogger(ChunkedInputStream.class);

  private DataInputStream din;
  private int unreadBytes = 0;  // Bytes remaining in the current chunk of data
  private byte[] singleByte = new byte[1];
  private boolean endOfData = false;
  private String id;

  public ChunkedInputStream(InputStream in, String id) {
    din = new DataInputStream(in);
    this.id = id;
    LOG.debug("Creating chunked input for {}", id);
  }

  @Override
  public void close() throws IOException {
    LOG.debug("{}: Closing chunked input.", id);
    din.close();
  }

  @Override
  public int read() throws IOException {
    int bytesRead = read(singleByte, 0, 1);
    return (bytesRead == -1) ? -1 : (int) singleByte[0];
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = 0;

    if (len < 0) {
      throw new IllegalArgumentException(id + ": Negative read length");
    } else if (len == 0) {
      return 0;
    }

    // If there is a current unread chunk, read from that, or else get the next chunk.
    if (unreadBytes == 0) {
      try {
        // Find the next chunk size
        unreadBytes = din.readInt();
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: Chunk size {}", id, unreadBytes);
        }
        if (unreadBytes == 0) {
          LOG.debug("{}: Hit end of data", id);
          endOfData = true;
          return -1;
        }
      } catch (IOException err) {
        throw new IOException(id + ": Error while attempting to read chunk length", err);
      }
    }

    int bytesToRead = Math.min(len, unreadBytes);
    try {
      din.readFully(b, off, bytesToRead);
    } catch (IOException err) {
      throw new IOException(id + ": Error while attempting to read " + bytesToRead + " bytes from current chunk", err);
    }
    unreadBytes -= bytesToRead;
    bytesRead += bytesToRead;

    return bytesRead;
  }

  public boolean isEndOfData() {
    return endOfData;
  }
}
