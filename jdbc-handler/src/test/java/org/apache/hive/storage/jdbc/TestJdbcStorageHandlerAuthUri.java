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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.storage.jdbc;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link JdbcStorageHandler#getURIForAuth(Table)}.
 */
public class TestJdbcStorageHandlerAuthUri {

  private static Table tableWith(Map<String, String> params) {
    Table table = new Table();
    table.setParameters(params);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo("serde", "serde.lib", new HashMap<>()));
    table.setSd(sd);
    return table;
  }

  private URI authUri(String jdbcUrl, String schema, String table) throws URISyntaxException {
    Map<String, String> params = new HashMap<>();
    params.put("hive.sql.database.type", "POSTGRES");
    params.put("hive.sql.jdbc.url", jdbcUrl);
    if (schema != null) {
      params.put("hive.sql.schema", schema);
    }
    if (table != null) {
      params.put("hive.sql.table", table);
    }
    return new JdbcStorageHandler().getURIForAuth(tableWith(params));
  }

  @Test
  public void testUnquotedTableUnchanged() throws Exception {
    URI uri = authUri("jdbc:postgresql://host:5432/db", null, "country");
    assertEquals("jdbc:postgresql://host:5432/db/country", uri.toString());
  }

  @Test
  public void testQuotedTableStripsQuotesPreservingCase() throws Exception {
    URI uri = authUri("jdbc:postgresql://host:5432/db", null, "\"Country\"");
    // The physical identifier keeps its original case, and the surrounding quotes are stripped.
    assertEquals("jdbc:postgresql://host:5432/db/Country", uri.toString());
  }

  @Test
  public void testQuotedTableWithSchemaOnlyEncodesTable() throws Exception {
    URI uri = authUri("jdbc:postgresql://host:5432/db", "\"World\"", "\"Country\"");
    assertEquals("jdbc:postgresql://host:5432/db/Country", uri.toString());
  }

  @Test
  public void testMissingTablePropertyFailsFast() {
    try {
      authUri("jdbc:postgresql://host:5432/db", null, null);
      org.junit.Assert.fail("Expected URISyntaxException");
    } catch (URISyntaxException e) {
      assertEquals("Missing required table property: hive.sql.table", e.getReason());
    }
  }

  @Test
  public void testTableWithSpecialCharactersIsUriSafe() throws Exception {
    // A quoted identifier may legally contain characters that are illegal in a raw URI; they must be encoded.
    URI uri = authUri("jdbc:postgresql://host:5432/db", null, "\"Odd/Name\"");
    // '/' -> %2F ; the resulting string is a valid URI.
    assertEquals("jdbc:postgresql://host:5432/db/Odd%2FName", uri.toString());
  }
}



