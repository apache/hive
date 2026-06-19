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

package org.apache.hadoop.hive.ql.anon.misc;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.hive.ql.anon.avro.BankDetails;
import org.apache.hadoop.hive.ql.anon.avro.Msg3;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.hive.ql.anon.utils.MessageUtils.getTestIps;

public class TestAvroSerDe {

  @Test
  public void test() throws IOException {
    Msg3 msg3 = Msg3.newBuilder()
      .setUserId(1)
      .setFirstName("John")
      .setLastName("Doe")
      .setEmail("john.doe@example.com")
      .setAddress("123 Main St.")
      .setCity("San Francisco")
      .setCountry("USA")
      .setTelephone("001")
      .setBirthDate("1/1/1")
      .setValue("v")
      .setIpList(getTestIps())
      .setBankDetails(BankDetails
        .newBuilder()
        .setBankName("bank")
        .setCardNum("33")
        .setPinCode("0000")
        .build()
      )
      .build();

    File file = new File("test.avro");
    if (file.exists()) {
      if (!file.delete()) {
        throw new IOException("Failed to delete test file");
      }
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(Msg3.getClassSchema());
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
    writer.write(msg3, encoder);
    encoder.flush();
    baos.flush();
    byte[] bytes = baos.toByteArray();
    System.out.println("bytes = " + bytes.length);


    DatumReader<SpecificRecordBase> reader = new SpecificDatumReader<>(Msg3.getClassSchema());
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    Msg3 out = (Msg3) reader.read(null, decoder);
    System.out.println("out = " + out);

    Assertions.assertEquals("John", out.getFirstName());
    Assertions.assertEquals("Doe", out.getLastName());
  }

}
