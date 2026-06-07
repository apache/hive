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

package org.apache.hadoop.hive.ql.anon.cmd;

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.FileType;
import org.apache.hadoop.hive.ql.anon.TestUtils;
import org.apache.hadoop.hive.ql.anon.builders.SelectStatementBuilder;
import org.apache.hadoop.hive.ql.anon.convert.JsonBodyConverter;
import org.apache.hadoop.hive.ql.anon.e2e.BaseEndToEndTest;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class TestAnonymizeTable extends BaseEndToEndTest {

  private final String policy = "tat_pol_" + Long.toString(System.nanoTime(), 36);

  private final String policyDsl =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE INT
      FOR SCHEMA 3
          ERASE country, city, telephone, ipList
      """;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { policy };
  }

  @Test
  public void testEraseBlanksTargetIdentityAndLeavesOthersUntouched()
      throws CommandProcessorException, IOException {
    this.internalFormat = ColumnInternalFormat.JSON;
    this.fileType = FileType.ORC;
    this.tblName = "t_anon_" + Long.toString(System.nanoTime(), 36);
    this.policyName = policy;

    create();
    truncate();
    insert();

    provisionAndAttach();

    final String selectCmd =
        new SelectStatementBuilder(tblName, mColName, oColName, bColName, internalFormat).build();
    final int otherUserId = userId + 1;

    final Msg3 otherBefore = singleIdentityRow(execute(selectCmd), otherUserId);

    anonymizeTable();

    final List<Object> after = execute(selectCmd);

    Assertions.assertEquals(7, after.size(), "the erase must preserve the row count");

    TestUtils.validateRowSet(after, internalFormat, userId);

    final Msg3 otherAfter = singleIdentityRow(after, otherUserId);
    Assertions.assertEquals(otherUserId, otherAfter.getUserId(),
        "the untargeted identity itself must be preserved");
    Assertions.assertEquals(otherBefore.getCountry(), otherAfter.getCountry(),
        "erasing identity 1 must not change identity 2's country");
    Assertions.assertEquals(otherBefore.getCity(), otherAfter.getCity(),
        "erasing identity 1 must not change identity 2's city");
    Assertions.assertEquals(otherBefore.getTelephone(), otherAfter.getTelephone(),
        "erasing identity 1 must not change identity 2's telephone");
    Assertions.assertEquals(otherBefore.getIpList(), otherAfter.getIpList(),
        "erasing identity 1 must not change identity 2's ipList");
  }

  private Msg3 singleIdentityRow(final List<Object> rows, final int uid) {
    Msg3 found = null;
    int n = 0;
    for (final Object o : rows) {
      final String[] cols = ((String) o).split("\\t");
      if (cols.length != 3) {
        continue;
      }
      final Msg3 msg = JsonBodyConverter.convert(cols[2], Msg3.class);
      if (msg.getUserId() == uid) {
        found = msg;
        n++;
      }
    }
    Assertions.assertEquals(1, n, "expected exactly one row for userId=" + uid);
    return found;
  }

  private void provisionAndAttach() throws CommandProcessorException, IOException {
    final Path policyFile = Files.createTempFile(policy + "_", ".erp");
    Files.write(policyFile, policyDsl.getBytes());
    execute("LOAD ERASURE POLICY " + policy + " FROM '" + policyFile + "'");
    execute("VALIDATE ERASURE POLICY " + policy);
    execute("ACTIVATE ERASURE POLICY " + policy);
    execute(("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s"
        + " WITH ( SCHEMA FIELD (%s), ROW LOCATOR (%s),"
        + " COLUMN FORMAT (JSON) )"
        + " RESOLUTION ( EXPLICIT )").formatted(policy, tblName, bColName, mColName, oColName));
  }
}
