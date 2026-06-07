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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.FileType;
import org.apache.hadoop.hive.ql.anon.TestUtils;
import org.apache.hadoop.hive.ql.anon.avro.Address;
import org.apache.hadoop.hive.ql.anon.avro.Msg4;
import org.apache.hadoop.hive.ql.anon.builders.SelectStatementBuilder;
import org.apache.hadoop.hive.ql.anon.convert.AvroBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.ProtobufBodyConverter;
import org.apache.hadoop.hive.ql.anon.e2e.BaseEndToEndTest;
import org.apache.hadoop.hive.ql.anon.proto.TestMessages;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRepeatedMessageE2E extends BaseEndToEndTest {

  private final String avroStarPolicy = "trm_av_star_" + Long.toString(System.nanoTime(), 36);
  private final String protoStarPolicy = "trm_pb_star_" + Long.toString(System.nanoTime(), 36);
  private final String avroFilterPolicy = "trm_av_de_" + Long.toString(System.nanoTime(), 36);
  private final String protoFilterPolicy = "trm_pb_de_" + Long.toString(System.nanoTime(), 36);

  private static final String WILDCARD_DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE INT
      FOR SCHEMA 5
          ERASE addressBookList[*]:street
      """;

  private static final String FILTER_DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE INT
      FOR SCHEMA 5
          ERASE addressBookList[country='DE']:street
      """;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { avroStarPolicy, protoStarPolicy, avroFilterPolicy, protoFilterPolicy };
  }

  @Test
  public void testWildcardEraseAvro() throws CommandProcessorException, IOException {
    runRepeatedErase(ColumnInternalFormat.AVRO, avroStarPolicy, WILDCARD_DSL, "t_rmavw_", false);
  }

  @Test
  public void testWildcardEraseProtobuf() throws CommandProcessorException, IOException {
    runRepeatedErase(ColumnInternalFormat.PROTOBUF, protoStarPolicy, WILDCARD_DSL, "t_rmpbw_", false);
  }

  @Test
  public void testFilterEraseAvro() throws CommandProcessorException, IOException {
    runRepeatedErase(ColumnInternalFormat.AVRO, avroFilterPolicy, FILTER_DSL, "t_rmavf_", true);
  }

  @Test
  public void testFilterEraseProtobuf() throws CommandProcessorException, IOException {
    runRepeatedErase(ColumnInternalFormat.PROTOBUF, protoFilterPolicy, FILTER_DSL, "t_rmpbf_", true);
  }

  private record RepeatedView(int userId, String country0, String street0,
                              String country1, String street1) {
  }

  private void runRepeatedErase(final ColumnInternalFormat fmt, final String policy,
                                final String policyDsl, final String tblPrefix,
                                final boolean filterOnly) throws CommandProcessorException, IOException {
    this.internalFormat = fmt;
    this.fileType = FileType.ORC;
    this.tblName = tblPrefix + Long.toString(System.nanoTime(), 36);
    this.policyName = policy;

    final int targetUserId = userId;
    final int otherUserId = userId + 1;

    create();
    truncate();
    insertMsg4(targetUserId, otherUserId);

    provisionAndAttach(policy, policyDsl);

    final String selectCmd =
        new SelectStatementBuilder(tblName, mColName, oColName, bColName, internalFormat).build();

    final List<Object> before = execute(selectCmd);
    Assertions.assertEquals(3, before.size(), fmt + ": fixture must seed 3 rows");
    final RepeatedView targetBefore = firstView(before, targetUserId);
    final RepeatedView otherBefore = firstView(before, otherUserId);
    Assertions.assertEquals("1 Main", targetBefore.street0(),
        fmt + ": fixture sanity — US street must start as '1 Main'");
    Assertions.assertEquals("2 Haupt", targetBefore.street1(),
        fmt + ": fixture sanity — DE street must start as '2 Haupt'");
    Assertions.assertEquals("1 Main", otherBefore.street0(),
        fmt + ": fixture sanity — control US street must start as '1 Main'");

    anonymizeTable();

    final List<Object> after = execute(selectCmd);
    Assertions.assertEquals(3, after.size(), fmt + ": the erase must preserve the row count");

    int matched = 0;
    int control = 0;
    for (final Object o : after) {
      final RepeatedView v = decodeRow(o);
      if (v == null) {
        continue;
      }
      if (v.userId() == targetUserId) {
        matched++;
        Assertions.assertEquals("US", v.country0(), fmt + ": element[0] country must be retained");
        Assertions.assertEquals("DE", v.country1(), fmt + ": element[1] country must be retained");
        Assertions.assertEquals("", v.street1(),
            fmt + ": element[1] (DE) street must be blanked");
        if (filterOnly) {
          Assertions.assertEquals("1 Main", v.street0(),
              fmt + ": element[0] (US) street must be retained under the [country='DE'] filter");
        } else {
          Assertions.assertEquals("", v.street0(),
              fmt + ": element[0] (US) street must be blanked under the [*] wildcard");
        }
      } else {
        control++;
        Assertions.assertEquals(otherUserId, v.userId(),
            fmt + ": only the control identity may appear besides the target");
        Assertions.assertEquals(otherBefore.country0(), v.country0(),
            fmt + ": erasing identity 1 must not change identity 2's element[0] country");
        Assertions.assertEquals(otherBefore.street0(), v.street0(),
            fmt + ": erasing identity 1 must not change identity 2's element[0] street");
        Assertions.assertEquals(otherBefore.country1(), v.country1(),
            fmt + ": erasing identity 1 must not change identity 2's element[1] country");
        Assertions.assertEquals(otherBefore.street1(), v.street1(),
            fmt + ": erasing identity 1 must not change identity 2's element[1] street");
      }
    }
    Assertions.assertEquals(2, matched, fmt + ": both target-identity rows must decode");
    Assertions.assertEquals(1, control, fmt + ": exactly one control-identity row must decode");
  }

  private void insertMsg4(final int targetUserId, final int otherUserId) throws CommandProcessorException {
    final List<String> commands =
        TestUtils.getInsertCommandsMsg4(tblName, targetUserId, otherUserId, internalFormat);
    for (final String cmd : commands) {
      final List<Object> lst = execute(cmd);
      Assertions.assertEquals(0, lst.size());
    }
  }

  private RepeatedView firstView(final List<Object> rows, final int uid) {
    for (final Object o : rows) {
      final RepeatedView v = decodeRow(o);
      if (v != null && v.userId() == uid) {
        return v;
      }
    }
    return Assertions.fail("no row found for userId=" + uid);
  }

  private RepeatedView decodeRow(final Object row) {
    final String[] cols = ((String) row).split("\\t");
    if (cols.length != 3) {
      return null;
    }
    final String hexBody = cols[2];
    if (internalFormat == ColumnInternalFormat.AVRO) {
      final Msg4 m = (Msg4) AvroBodyConverter.convert(hexBody, Msg4.class);
      final List<Address> book = m.getAddressBookList();
      return new RepeatedView(m.getUserId(),
          String.valueOf(book.get(0).getCountry()),
          String.valueOf(book.get(0).getStreet()),
          String.valueOf(book.get(1).getCountry()),
          String.valueOf(book.get(1).getStreet()));
    }
    final TestMessages.Msg4 m = ProtobufBodyConverter.convert(hexBody, TestMessages.Msg4.class);
    return new RepeatedView(m.getUserId(),
        m.getAddressBook(0).getCountry(),
        m.getAddressBook(0).getStreet(),
        m.getAddressBook(1).getCountry(),
        m.getAddressBook(1).getStreet());
  }

  private void provisionAndAttach(final String policy, final String policyDsl)
      throws CommandProcessorException, IOException {
    final Path policyFile = Files.createTempFile(policy + "_", ".erp");
    Files.write(policyFile, policyDsl.getBytes());
    execute("LOAD ERASURE POLICY " + policy + " FROM '" + policyFile + "'");
    execute("VALIDATE ERASURE POLICY " + policy);
    execute("ACTIVATE ERASURE POLICY " + policy);
    execute(("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s"
        + " WITH ( SCHEMA FIELD (%s), ROW LOCATOR (%s),"
        + " COLUMN FORMAT (%s) )"
        + " RESOLUTION ( EXPLICIT )").formatted(policy, tblName, bColName, mColName, oColName,
        internalFormat.name()));
  }
}
