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
 */package org.apache.hadoop.hive.ql.optimizer.calcite;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestHiveRelJsonSchemaReader {
  private static final Path TPCDS_JSON_PATH =
      Paths.get(HiveTestEnvSetup.HIVE_ROOT, "ql/src/test/results/clientpositive/perf/tpcds30tb/json");

  @Test
  void testReadSchemaFromTpcdsQuery1() throws IOException {
    Path iFile = TPCDS_JSON_PATH.resolve("query1.q.out");
    String jsonContent = new String(Files.readAllBytes(iFile), Charset.defaultCharset());
    RelOptSchema schema = HiveRelJsonSchemaReader.read(jsonContent, new HiveConf(), new HiveTypeFactory());
    Set<TpcdsTable> validTables =
        ImmutableSet.of(TpcdsTable.CUSTOMER, TpcdsTable.STORE, TpcdsTable.DATE_DIM, TpcdsTable.STORE_RETURNS);
    for (TpcdsTable t : validTables) {
      RelOptTable table = schema.getTableForMember(Arrays.asList("default", t.name().toLowerCase()));
      assertNotNull(table, "Table " + t.name() + " not found in schema");
      assertEquals(t.type(schema.getTypeFactory()), table.getRowType());
      assertEquals(t.rowCount(), table.getRowCount(), 1d);
    }
  }

}
