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

package org.apache.hadoop.hive.serde2.lazydio;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBooleanObjectInspector;

/**
 * LazyBooleanBinary for storing a boolean value as an BooleanWritable. This class complements class
 * LazyBoolean. It's primary difference is the {@link #init(ByteArrayRef, int, int)} method, which
 * reads the boolean value stored from the default binary format.
 */
public class LazyDioBoolean extends LazyBoolean {

  private ByteStream.Input in;
  private DataInputStream din;

  public LazyDioBoolean(LazyBooleanObjectInspector oi) {
    super(oi);
  }

  public LazyDioBoolean(LazyDioBoolean copy) {
    super(copy);
  }

  /* (non-Javadoc)
   * This provides a LazyBoolean like class which can be initialized from data stored in a
   * binary format.
   *
   * @see org.apache.hadoop.hive.serde2.lazy.LazyObject#init
   *        (org.apache.hadoop.hive.serde2.lazy.ByteArrayRef, int, int)
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {

    boolean value = false;

    try {
      in = new ByteStream.Input(bytes.getData(), start, length);
      din = new DataInputStream(in);
      value = din.readBoolean();
      data.set(value);
      isNull = false;
    } catch (IOException e) {
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
