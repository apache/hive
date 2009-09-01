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

package org.apache.hadoop.hive.contrib.util.typedbytes;

import java.io.IOException;
import java.io.InputStream;
import java.io.DataInputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.ql.io.NonSyncDataOutputBuffer;
import org.apache.hadoop.hive.ql.exec.RecordReader;

public class TypedBytesRecordReader implements RecordReader {

  private DataInputStream din;
  private TypedBytesWritableInput tbIn;

  NonSyncDataOutputBuffer barrStr = new NonSyncDataOutputBuffer();
  TypedBytesWritableOutput tbOut = new TypedBytesWritableOutput(barrStr);

  ArrayList<Writable> row = new ArrayList<Writable>(0);

  public void initialize(InputStream in, Configuration conf) throws IOException {
    din = new DataInputStream(in);
    tbIn = new TypedBytesWritableInput(din);
  }

  public Writable createRow() throws IOException {
    BytesWritable retWrit = new BytesWritable();
    return retWrit;
  }

  private Writable allocateWritable(Type type) {
    switch (type) {
    case BYTE:
      return new ByteWritable();
    case BOOL:
      return new BooleanWritable();
    case INT:
      return new IntWritable();
    case SHORT:
      return new ShortWritable();
    case LONG:
      return new LongWritable();
    case FLOAT:
      return new FloatWritable();
    case DOUBLE:
      return new DoubleWritable();
    case STRING:
      return new Text();
     default:
       assert false; // not supported
    }
    return null;
  }
  
  public int next(Writable data) throws IOException {
    int pos = 0;
    barrStr.reset();

    while (true) {
      Type type = tbIn.readTypeCode();
      
      // it was a empty stream
      if (type == null)
        return -1;
      
      if (type == Type.ENDOFRECORD) {
        tbOut.writeEndOfRecord();
        if (barrStr.getLength() > 0)
          ((BytesWritable)data).set(barrStr.getData(), 0, barrStr.getLength());
        return barrStr.getLength();
      }
    
      if (pos >= row.size()) {
        Writable wrt = allocateWritable(type);
        assert pos == row.size();
        row.add(wrt);
      }
     
      switch (type) {
        case BYTE: {
          ByteWritable bw = (ByteWritable)row.get(pos);
          tbIn.readByte(bw);
          tbOut.writeByte(bw);
          break;
        }
        case BOOL: {
          BooleanWritable bw = (BooleanWritable)row.get(pos);
          tbIn.readBoolean(bw);
          tbOut.writeBoolean(bw);
          break;
        }
        case INT: {
          IntWritable iw = (IntWritable)row.get(pos);
          tbIn.readInt(iw);
          tbOut.writeInt(iw);
          break;
        }
        case SHORT: {
          ShortWritable sw = (ShortWritable)row.get(pos);
          tbIn.readShort(sw);
          tbOut.writeShort(sw);
          break;
        }        
        case LONG: {
          LongWritable lw = (LongWritable)row.get(pos);
          tbIn.readLong(lw);
          tbOut.writeLong(lw);
          break;
        }
        case FLOAT: {
          FloatWritable fw = (FloatWritable)row.get(pos);
          tbIn.readFloat(fw);
          tbOut.writeFloat(fw);
          break;
        }
        case DOUBLE: {
          DoubleWritable dw = (DoubleWritable)row.get(pos);
          tbIn.readDouble(dw);
          tbOut.writeDouble(dw);
          break;
        }
        case STRING: {
          Text txt = (Text)row.get(pos);
          tbIn.readText(txt);
          tbOut.writeText(txt);
          break;
        }
        default:
          assert false;  // should never come here
      }
    
      pos++;
    }
  }
  
  public void close() throws IOException {
    if (din != null)
      din.close();
  }
}
