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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.io.Writable;

public class ResultWritable implements Writable {

  private Result result;

  public ResultWritable() {

  }
  public ResultWritable(Result result) {
    this.result = result;
  }

  public Result getResult() {
    return result;
  }
  public void setResult(Result result) {
    this.result = result;
  }
  @Override
  public void readFields(final DataInput in)
  throws IOException {
    ClientProtos.Result protosResult = ClientProtos.Result.parseDelimitedFrom(DataInputInputStream.from(in));
    int size = in.readInt();
    if(size < 0) {
      throw new IOException("Invalid size " + size);
    }
    Cell[] kvs = new Cell[size];
    for (int i = 0; i < kvs.length; i++) {
      kvs[i] = KeyValue.create(in);
    }
    result = ProtobufUtil.toResult(protosResult, CellUtil.createCellScanner(kvs));
  }
  @Override
  public void write(final DataOutput out)
  throws IOException {
    ProtobufUtil.toResultNoData(result).writeDelimitedTo(DataOutputOutputStream.from(out));
    out.writeInt(result.size());
    for(Cell cell : result.listCells()) {
      KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
      KeyValue.write(kv, out);
    }
  }
}
