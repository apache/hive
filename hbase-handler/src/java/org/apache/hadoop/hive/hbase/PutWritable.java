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

package org.apache.hadoop.hive.hbase;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.io.Writable;

public class PutWritable implements Writable {

  private Put put;

  public PutWritable() {

  }
  public PutWritable(Put put) {
    this.put = put;
  }
  public Put getPut() {
    return put;
  }
  @Override
  public void readFields(final DataInput in)
  throws IOException {
    ClientProtos.MutationProto putProto = ClientProtos.MutationProto.parseDelimitedFrom(DataInputInputStream.from(in));
    int size = in.readInt();
    if(size < 0) {
      throw new IOException("Invalid size " + size);
    }
    Cell[] kvs = new Cell[size];
    for (int i = 0; i < kvs.length; i++) {
      kvs[i] = KeyValue.create(in);
    }
    put = ProtobufUtil.toPut(putProto, CellUtil.createCellScanner(kvs));
  }
  @Override
  public void write(final DataOutput out)
  throws IOException {
    ProtobufUtil.toMutationNoData(MutationType.PUT, put).writeDelimitedTo(DataOutputOutputStream.from(out));
    out.writeInt(put.size());
    CellScanner scanner = put.cellScanner();
    while(scanner.advance()) {
      KeyValue kv = KeyValueUtil.ensureKeyValue(scanner.current());
      KeyValue.write(kv, out);
    }
  }
}
