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

package org.apache.hadoop.hive.ql.index.bitmap;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * An ObjectOutput that allows conversion from an EWAH-compressed bitmap
 * to an List of LongWritable.
 */
public class BitmapObjectOutput implements ObjectOutput {
  ArrayList<LongWritable> buffer = new ArrayList<LongWritable>();

  public List<LongWritable> list() {
    return buffer;
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flush() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] arg0, int arg1, int arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeObject(Object arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeBoolean(boolean arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeByte(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeBytes(String arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeChar(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeChars(String arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeDouble(double v) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeFloat(float v) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeInt(int v) throws IOException {
    buffer.add(new LongWritable(v));
  }

  @Override
  public void writeLong(long v) throws IOException {
    buffer.add(new LongWritable(v));
  }

  @Override
  public void writeShort(int v) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeUTF(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

}
