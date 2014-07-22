/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.rmi.server.UID;

import org.apache.avro.generic.GenericData;

class Utils {
  // Force Avro to serialize and de-serialize the record to make sure it has a
  // chance to muck with the bytes and we're working against real Avro data.
  public static AvroGenericRecordWritable
  serializeAndDeserializeRecord(GenericData.Record record) throws IOException {
    AvroGenericRecordWritable garw = new AvroGenericRecordWritable(record);
    garw.setRecordReaderID(new UID());
    // Assuming file schema is the same as record schema for testing purpose.
    garw.setFileSchema(record.getSchema());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream daos = new DataOutputStream(baos);
    garw.write(daos);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dais = new DataInputStream(bais);

    AvroGenericRecordWritable garw2 = new AvroGenericRecordWritable();
    garw2.setRecordReaderID(new UID());
    garw2.readFields(dais);
    return garw2;
  }
}
