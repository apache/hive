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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.io.ByteArrayOutputStream;

/**
 * An output stream that will only accept the first N bytes of data.
 */
public class MaxByteArrayOutputStream extends ByteArrayOutputStream {
  /**
   * The max number of bytes stored.
   */
  private int maxBytes;

  /**
   * The number of bytes currently stored.
   */
  private int nBytes;

  /**
   * Create.
   */
  public MaxByteArrayOutputStream(int maxBytes) {
    this.maxBytes = maxBytes;
    nBytes = 0;
  }

  /**
   * Writes the specified byte to this byte array output stream.
   * Any bytes after the first maxBytes will be ignored.
   *
   * @param   b   the byte to be written.
   */
  public synchronized void write(int b) {
    if (nBytes < maxBytes) {
      ++nBytes;
      super.write(b);
    }
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array
   * starting at offset <code>off</code> to this byte array output stream.
   * Any bytes after the first maxBytes will be ignored.
   *
   * @param   b     the data.
   * @param   off   the start offset in the data.
   * @param   len   the number of bytes to write.
   */
  public synchronized void write(byte b[], int off, int len) {
    int storable = Math.min(maxBytes - nBytes, len);
    if (storable > 0) {
      nBytes += storable;
      super.write(b, off, storable);
    }
  }


}
