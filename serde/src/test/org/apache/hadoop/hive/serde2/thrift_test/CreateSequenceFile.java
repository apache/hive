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

package org.apache.hadoop.hive.serde2.thrift_test;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.thrift.test.Complex;
import org.apache.hadoop.hive.serde2.thrift.test.IntString;
import org.apache.hadoop.hive.serde2.thrift.test.PropValueUnion;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * CreateSequenceFile.
 *
 */
public final class CreateSequenceFile {

  private CreateSequenceFile() {
    // prevent instantiation
  }

  public static void usage() {
    System.out.println("Usage: CreateSequenceFile <output_sequencefile>");
    System.exit(1);
  }

  /**
   * ThriftSerializer.
   *
   */
  public static class ThriftSerializer {

    private ByteStream.Output bos;
    private TProtocol outProtocol;

    public ThriftSerializer() {
      bos = new ByteStream.Output();
      TIOStreamTransport outTransport = null;
      try {
        outTransport = new TIOStreamTransport(bos);
      } catch (TTransportException e) {
        e.printStackTrace();
      }
      TProtocolFactory outFactory = new TBinaryProtocol.Factory();
      outProtocol = outFactory.getProtocol(outTransport);
    }

    private BytesWritable bw = new BytesWritable();

    public BytesWritable serialize(TBase base) throws TException {
      bos.reset();
      base.write(outProtocol);
      bw.set(bos.getData(), 0, bos.getLength());
      return bw;
    }
  }

  public static void main(String[] args) throws Exception {

    // Read parameters
    int lines = 10;
    List<String> extraArgs = new ArrayList<String>();
    for (int ai = 0; ai < args.length; ai++) {
      if (args[ai].equals("-line") && ai + 1 < args.length) {
        lines = Integer.parseInt(args[ai + 1]);
        ai++;
      } else {
        extraArgs.add(args[ai]);
      }
    }
    if (extraArgs.size() != 1) {
      usage();
    }

    JobConf conf = new JobConf(CreateSequenceFile.class);

    ThriftSerializer serializer = new ThriftSerializer();

    // Open files
    SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf),
        conf, new Path(extraArgs.get(0)), BytesWritable.class,
        BytesWritable.class);

    // write to file
    BytesWritable key = new BytesWritable();

    Random rand = new Random(20081215);

    for (int i = 0; i < lines; i++) {

      ArrayList<Integer> alist = new ArrayList<Integer>();
      alist.add(i);
      alist.add(i * 2);
      alist.add(i * 3);
      ArrayList<String> slist = new ArrayList<String>();
      slist.add("" + i * 10);
      slist.add("" + i * 100);
      slist.add("" + i * 1000);
      ArrayList<IntString> islist = new ArrayList<IntString>();
      islist.add(new IntString(i * i, "" + i * i * i, i));
      HashMap<String, String> hash = new HashMap<String, String>();
      hash.put("key_" + i, "value_" + i);
      Map<String, Map<String, Map<String,PropValueUnion>>> unionMap = new HashMap<String, Map<String, Map<String,PropValueUnion>>>();
      Map<String, Map<String, PropValueUnion>> erMap = new HashMap<String, Map<String, PropValueUnion>>();
      Map<String, PropValueUnion> attrMap = new HashMap<String, PropValueUnion>();

      erMap.put("erVal" + i, attrMap);
      attrMap.put("value_" + i, PropValueUnion.doubleValue(1.0));
      unionMap.put("key_" + i,  erMap);

      Complex complex = new Complex(rand.nextInt(), "record_"
          + String.valueOf(i), alist, slist, islist, hash, unionMap, PropValueUnion.stringValue("test" + i),
          PropValueUnion.unionMStringString(hash), PropValueUnion.lString(slist));

      Writable value = serializer.serialize(complex);
      writer.append(key, value);
    }

    // Add an all-null record
    Complex complex = new Complex(0, null, null, null, null, null, null, null, null, null);
    Writable value = serializer.serialize(complex);
    writer.append(key, value);

    // Close files
    writer.close();
  }

}
