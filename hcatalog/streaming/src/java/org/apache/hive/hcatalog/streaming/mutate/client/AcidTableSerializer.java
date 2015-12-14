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
package org.apache.hive.hcatalog.streaming.mutate.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to serialize/deserialize {@link AcidTable AcidTables} into strings so that they can be easily transported as
 * {@link Configuration} properties.
 */
public class AcidTableSerializer {

  private static final Logger LOG = LoggerFactory.getLogger(AcidTableSerializer.class);

  /* Allow for improved schemes. */
  private static final String PROLOG_V1 = "AcidTableV1:";

  /** Returns a base 64 encoded representation of the supplied {@link AcidTable}. */
  public static String encode(AcidTable table) throws IOException {
    DataOutputStream data = null;
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try {
      data = new DataOutputStream(bytes);
      data.writeUTF(table.getDatabaseName());
      data.writeUTF(table.getTableName());
      data.writeBoolean(table.createPartitions());
      if (table.getTransactionId() <= 0) {
        LOG.warn("Transaction ID <= 0. The recipient is probably expecting a transaction ID.");
      }
      data.writeLong(table.getTransactionId());
      data.writeByte(table.getTableType().getId());

      Table metaTable = table.getTable();
      if (metaTable != null) {
        byte[] thrift = new TSerializer(new TCompactProtocol.Factory()).serialize(metaTable);
        data.writeInt(thrift.length);
        data.write(thrift);
      } else {
        LOG.warn("Meta store table is null. The recipient is probably expecting an instance.");
        data.writeInt(0);
      }
    } catch (TException e) {
      throw new IOException("Error serializing meta store table.", e);
    } finally {
      data.close();
    }

    return PROLOG_V1 + new String(Base64.encodeBase64(bytes.toByteArray()), Charset.forName("UTF-8"));
  }

  /** Returns the {@link AcidTable} instance decoded from a base 64 representation. */
  public static AcidTable decode(String encoded) throws IOException {
    if (!encoded.startsWith(PROLOG_V1)) {
      throw new IllegalStateException("Unsupported version.");
    }
    encoded = encoded.substring(PROLOG_V1.length());

    byte[] decoded = Base64.decodeBase64(encoded);
    AcidTable table = null;
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(decoded))) {
      String databaseName = in.readUTF();
      String tableName = in.readUTF();
      boolean createPartitions = in.readBoolean();
      long transactionId = in.readLong();
      TableType tableType = TableType.valueOf(in.readByte());
      int thriftLength = in.readInt();

      table = new AcidTable(databaseName, tableName, createPartitions, tableType);
      table.setTransactionId(transactionId);

      Table metaTable = null;
      if (thriftLength > 0) {
        metaTable = new Table();
        try {
          byte[] thriftEncoded = new byte[thriftLength];
          in.readFully(thriftEncoded, 0, thriftLength);
          new TDeserializer(new TCompactProtocol.Factory()).deserialize(metaTable, thriftEncoded);
          table.setTable(metaTable);
        } catch (TException e) {
          throw new IOException("Error deserializing meta store table.", e);
        }
      }
    }
    return table;
  }

}
