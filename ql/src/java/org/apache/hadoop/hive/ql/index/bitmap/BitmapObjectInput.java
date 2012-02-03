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
import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;

/**
 * An ObjectInput that allows for conversion from an List of LongWritable
 * to an EWAH-compressed bitmap.
 */
public class BitmapObjectInput implements ObjectInput {
  Iterator<LongWritable> bufferIter;
  List<LongWritable> buffer;

  public BitmapObjectInput() {
    buffer = new ArrayList<LongWritable>();
    bufferIter = buffer.iterator();
  }

  public BitmapObjectInput(List<LongWritable> l) {
    readFromList(l);
  }

  public void readFromList(List<LongWritable> l) {
    buffer = l;
    bufferIter = buffer.iterator();
  }

  @Override
  public int available() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public int read() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] arg0, int arg1, int arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object readObject() throws ClassNotFoundException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long skip(long arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean readBoolean() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte readByte() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public char readChar() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public double readDouble() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public float readFloat() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFully(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFully(byte[] arg0, int arg1, int arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readInt() throws IOException {
    if (bufferIter.hasNext()) {
      LongObjectInspector loi = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      Long l = PrimitiveObjectInspectorUtils.getLong(bufferIter.next(), loi);
      return l.intValue();
      //return bufferIter.next().intValue();
    }
    else {
      throw new IOException();
    }
  }

  @Override
  public String readLine() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long readLong() throws IOException {
    //LongObjectInspector loi = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    if (bufferIter.hasNext()) {
      LongObjectInspector loi = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      return PrimitiveObjectInspectorUtils.getLong(bufferIter.next(), loi);
      //return bufferIter.next();
    }
    else {
      throw new IOException();
    }
  }

  @Override
  public short readShort() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readUTF() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int skipBytes(int n) throws IOException {
    throw new UnsupportedOperationException();
  }


}
