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

package org.apache.hadoop.hive.serde2.lazydio;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyByteObjectInspector;

/**
 * LazyByteBinary for storing a byte value as a ByteWritable. This class complements class
 * LazyByte. It's primary difference is the {@link #init(ByteArrayRef, int, int)} method, which
 * reads the raw byte value stored.
 */
public class LazyDioByte extends LazyByte {

  private ByteStream.Input in;
  private DataInputStream din;

  public LazyDioByte(LazyByteObjectInspector oi) {
    super(oi);
  }

  public LazyDioByte(LazyDioByte copy) {
    super(copy);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {

    byte value = 0;

    try {
      in = new ByteStream.Input(bytes.getData(), start, length);
      din = new DataInputStream(in);
      value = din.readByte();
      data.set(value);
      isNull = false;
    } catch (Exception e) {
      isNull = true;
    } finally {
      try {
        din.close();
      } catch (IOException e) {
        // swallow exception
      }
      try {
        in.close();
      } catch (IOException e) {
        // swallow exception
      }
    }
  }
}
