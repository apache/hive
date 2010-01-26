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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;

/**
 * Read from a binary stream and treat each 1000 bytes (configurable via 
 * hive.binary.record.max.length) as a record.  The last record before the 
 * end of stream can have less than 1000 bytes. 
 */
public class BinaryRecordReader implements RecordReader {

  private InputStream in;
  private BytesWritable bytes;
  private int maxRecordLength;

  public void initialize(InputStream in, Configuration conf, Properties tbl)
      throws IOException {
    this.in = in;
    maxRecordLength = conf.getInt("hive.binary.record.max.length", 1000);
  }

  public Writable createRow() throws IOException {
    bytes = new BytesWritable();
    bytes.setCapacity(maxRecordLength);
    return bytes;
  }

  public int next(Writable row) throws IOException {
    int recordLength = in.read(bytes.get(), 0, maxRecordLength);
    if (recordLength >= 0) {
      bytes.setSize(recordLength);
    }
    return recordLength;
  }

  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
