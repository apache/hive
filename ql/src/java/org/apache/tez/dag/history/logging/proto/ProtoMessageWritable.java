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

package org.apache.tez.dag.history.logging.proto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.protobuf.ExtensionRegistryLite;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public class ProtoMessageWritable<T extends MessageLite> implements Writable {
  private T message;
  private final Parser<T> parser;
  private DataOutputStream dos;
  private CodedOutputStream cos;
  private DataInputStream din;
  private CodedInputStream cin;

  ProtoMessageWritable(Parser<T> parser) {
    this.parser = parser;
  }

  public T getMessage() {
    return message;
  }

  public void setMessage(T message) {
    this.message = message;
  }

  private static class DataOutputStream extends OutputStream {
    DataOutput out;
    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (dos == null) {
      dos = new DataOutputStream();
      cos = CodedOutputStream.newInstance(dos);
    }
    dos.out = out;
    cos.writeMessageNoTag(message);
    cos.flush();
  }

  private static class DataInputStream extends InputStream {
    DataInput in;
    @Override
    public int read() throws IOException {
      try {
        return in.readUnsignedByte();
      } catch (EOFException e) {
        return -1;
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (din == null) {
      din = new DataInputStream();
      cin = CodedInputStream.newInstance(din);
      cin.setSizeLimit(Integer.MAX_VALUE);
    }
    din.in = in;
    message = cin.readMessage(parser, ExtensionRegistryLite.newInstance());
  }
}
