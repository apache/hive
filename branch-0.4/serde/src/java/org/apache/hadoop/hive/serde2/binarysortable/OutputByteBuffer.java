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
package org.apache.hadoop.hive.serde2.binarysortable;

import java.util.Arrays;

/**
 * This class is much more efficient than ByteArrayOutputStream
 * because none of the methods are synchronized.
 */
public class OutputByteBuffer {
  
  byte[] data = new byte[128];
  int length;
  
  /**
   * Reset the byte buffer.
   */
  public void reset() {
    length = 0;
  }
  
  /**
   * Write one byte to the byte buffer.
   * Final method to help inlining.
   * @param invert whether we want to invert all the bits. 
   */
  public final void write(byte b, boolean invert) {
    if (length == data.length) {
      data = Arrays.copyOf(data, data.length*2);
    }
    if (invert) {
      data[length++] = (byte)(0xff ^ b);
    } else {
      data[length++] = b;
    }
  }
  
  /**
   * Returns the underlying byte array.
   */
  public final byte[] getData() {
    return data;
  }
  
  /**
   * Returns the current length.
   */
  public final int getLength() {
    return length;
  }

  /**
   * Return the bytes in hex format.
   */
  public String dumpHex() {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<length; i++) {
      byte b = data[i];
      int v = (b<0 ? 256 + b : b);
      sb.append(String.format("x%02x", v));
    }
    return sb.toString();
  }
  
}
