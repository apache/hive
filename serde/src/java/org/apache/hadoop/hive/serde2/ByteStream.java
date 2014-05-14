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

package org.apache.hadoop.hive.serde2;

import java.io.IOException;

import org.apache.hadoop.hive.common.io.NonSyncByteArrayInputStream;
import org.apache.hadoop.hive.common.io.NonSyncByteArrayOutputStream;

/**
 * Extensions to bytearrayinput/output streams.
 */
public class ByteStream {
  /**
   * Input.
   *
   */
  public static class Input extends NonSyncByteArrayInputStream {
    public byte[] getData() {
      return buf;
    }

    public int getCount() {
      return count;
    }

    public void reset(byte[] argBuf, int argCount) {
      buf = argBuf;
      mark = pos = 0;
      count = argCount;
    }

    public Input() {
      super(new byte[1]);
    }

    public Input(byte[] buf) {
      super(buf);
    }

    public Input(byte[] buf, int offset, int length) {
      super(buf, offset, length);
    }
  }

  /**
   * Output.
   *
   */
  public static final class Output
    extends NonSyncByteArrayOutputStream implements RandomAccessOutput {
    @Override
    public byte[] getData() {
      return buf;
    }

    public Output() {
      super();
    }

    public Output(int size) {
      super(size);
    }

    @Override
    public void writeInt(long offset, int value) {
      int offset2 = (int)offset;
      getData()[offset2++] = (byte) (value >> 24);
      getData()[offset2++] = (byte) (value >> 16);
      getData()[offset2++] = (byte) (value >> 8);
      getData()[offset2] = (byte) (value);
    }

    @Override
    public void reserve(int byteCount) {
      for (int i = 0; i < byteCount; ++i) {
        write(0);
      }
    }
  }

  public static interface RandomAccessOutput {
    public void writeInt(long offset, int value);
    public void reserve(int byteCount);
    public void write(int b);
    public void write(byte b[]) throws IOException;
    public void write(byte b[], int off, int len);
    public int getLength();
  }
}
