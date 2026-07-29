/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.metastore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gzip-compressed Thrift compact encoding of {@link Table}. */
public final class TableBlobCodec {
  private static final Logger LOG = LoggerFactory.getLogger(TableBlobCodec.class);
  private TableBlobCodec() {}

  public static byte[] encode(Table table) {
    try {
      ByteArrayOutputStream raw = new ByteArrayOutputStream();
      table.write(new TCompactProtocol(new TIOStreamTransport(raw)));
      return gzip(raw.toByteArray());
    } catch (IOException | TException e) {
      LOG.warn("Error serializing the table, message: {}, this shouldn't happen", e.getMessage(), e);
      return null;
    }
  }

  public static Table decode(byte[] compressed) throws IOException {
    try {
      Table table = new Table();
      table.read(new TCompactProtocol(new TIOStreamTransport(new ByteArrayInputStream(gunzip(compressed)))));
      return table;
    } catch (TException e) {
      throw new IOException("Failed to deserialize table", e);
    }
  }

  private static byte[] gzip(byte[] input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(input.length);
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(input);
    }
    return out.toByteArray();
  }

  private static byte[] gunzip(byte[] input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(input.length * 2);
    try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(input))) {
      gzip.transferTo(out);
    }
    return out.toByteArray();
  }
}
