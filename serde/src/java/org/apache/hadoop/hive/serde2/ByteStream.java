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

package org.apache.hadoop.hive.serde2;

import java.io.IOException;
import java.util.Arrays;

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

    public Input() {
      super();
    }

    public Input(byte[] buf) {
      super(buf);
    }

    public byte[] getData() {
      return buf;
    }

    public int getCount() {
      return count;
    }

    public void reset(byte[] buf, int count) {
      super.buf = buf;
      super.count = count;
      super.mark = super.pos = 0;
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
    
    private static final byte[] RESERVE_INT = { 0x00, 0x00, 0x00, 0x00 };

    public Output() {
      super();
    }

    public Output(int size) {
      super(size);
    }

    @Override
    public byte[] getData() {
      return buf;
    }

    @Override
    public void writeInt(long offset, int value) {
      int i = (int) offset;
      buf[i + 0] = (byte) (value >> 24);
      buf[i + 1] = (byte) (value >> 16);
      buf[i + 2] = (byte) (value >> 8);
      buf[i + 3] = (byte) (value);
    }

    @Override
    public void writeByte(long offset, byte value) {
      buf[(int) offset] = value;
    }

    /**
     * Optimize for the common cases:
     * <ul>
     *   <li>Reserve 1 byte</li>
     *   <li>Reserve 1 int (4 bytes)</li>
     * </ul>
     */
    @Override
    public void reserve(int byteCount) {
      switch (byteCount) {
      case 0:
        break;
      case 1:
        write(0);
        break;
      case 4:
        write(RESERVE_INT, 0, 4);
        break;
      default:
        for (int i = 0; i < byteCount; ++i) {
          write(0);
        }
      }
    }

    public boolean arraysEquals(Output output) {
      return Arrays.equals(super.buf, output.buf);
    }
  }

  public static interface RandomAccessOutput {
    public void writeByte(long offset, byte value);

    public void writeInt(long offset, int value);

    public void reserve(int byteCount);

    public void write(int b);

    public void write(byte b[]) throws IOException;

    public void write(byte b[], int off, int len);

    public int getLength();
  }
}
