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
import org.apache.hadoop.hive.ql.anon.avro.Msg3;
import org.apache.hadoop.hive.ql.anon.builders.SelectStatementBuilder;
import org.apache.hadoop.hive.ql.anon.convert.AvroBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.ProtobufBodyConverter;
import org.apache.hadoop.hive.ql.anon.e2e.BaseEndToEndTest;
import org.apache.hadoop.hive.ql.anon.proto.TestMessages;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
public class TestNestedFormatsE2E extends BaseEndToEndTest {

  private final String avroPolicy = "tnf_av_" + Long.toString(System.nanoTime(), 36);
  private final String protoPolicy = "tnf_pb_" + Long.toString(System.nanoTime(), 36);

  private static final String POLICY_DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE INT
      FOR SCHEMA 3
          ERASE bankDetails:bankName
      """;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { avroPolicy, protoPolicy };
  }

  @Test
  public void testNestedEraseAvro() throws CommandProcessorException, IOException {
    runNestedErase(ColumnInternalFormat.AVRO, avroPolicy, "t_nfav_");
  }

  @Test
  public void testNestedEraseProtobuf() throws CommandProcessorException, IOException {
    runNestedErase(ColumnInternalFormat.PROTOBUF, protoPolicy, "t_nfpb_");
  }

  private record NestedView(int userId, String bankName, String cardNum, String pinCode,
                            String country, String telephone) {
  }

  private void runNestedErase(final ColumnInternalFormat fmt, final String policy,
                              final String tblPrefix) throws CommandProcessorException, IOException {
    this.internalFormat = fmt;
    this.fileType = FileType.ORC;
    this.tblName = tblPrefix + Long.toString(System.nanoTime(), 36);
    this.policyName = policy;

    create();
    truncate();
    insert();

    provisionAndAttach(policy);

    final String selectCmd =
        new SelectStatementBuilder(tblName, mColName, oColName, bColName, internalFormat).build();
    final int otherUserId = userId + 1;

    final List<Object> before = execute(selectCmd);
    Assertions.assertEquals(7, before.size(), fmt + ": fixture must seed 7 rows");
    final NestedView targetBefore = firstView(before, userId);
    final NestedView otherBefore = firstView(before, otherUserId);
    Assertions.assertNotEquals("", targetBefore.bankName(),
        fmt + ": fixture sanity — the nested target leaf must start non-empty");
    Assertions.assertNotEquals("", otherBefore.bankName(),
        fmt + ": fixture sanity — the control identity's nested leaf must start non-empty");

    anonymizeTable();

    final List<Object> after = execute(selectCmd);
    Assertions.assertEquals(7, after.size(), fmt + ": the erase must preserve the row count");

    int matched = 0;
    int control = 0;
    for (final Object o : after) {
      final NestedView v = decodeRow(o);
      if (v == null) {
        continue;
      }
      if (v.userId() == userId) {
        matched++;
        Assertions.assertEquals("", v.bankName(),
            fmt + ": nested target bankDetails:bankName must be blanked");
        Assertions.assertEquals(targetBefore.cardNum(), v.cardNum(),
            fmt + ": nested sibling cardNum must be unchanged");
        Assertions.assertEquals(targetBefore.pinCode(), v.pinCode(),
            fmt + ": nested sibling pinCode must be unchanged");
        Assertions.assertEquals(targetBefore.country(), v.country(),
            fmt + ": top-level non-targeted country must be unchanged");
        Assertions.assertEquals(targetBefore.telephone(), v.telephone(),
            fmt + ": top-level non-targeted telephone must be unchanged");
      } else {
        control++;
        Assertions.assertEquals(otherUserId, v.userId(),
            fmt + ": only the control identity may appear besides the target");
        Assertions.assertEquals(otherBefore.bankName(), v.bankName(),
            fmt + ": erasing identity 1 must not change identity 2's nested bankName");
        Assertions.assertEquals(otherBefore.cardNum(), v.cardNum(),
            fmt + ": erasing identity 1 must not change identity 2's nested cardNum");
        Assertions.assertEquals(otherBefore.pinCode(), v.pinCode(),
            fmt + ": erasing identity 1 must not change identity 2's nested pinCode");
        Assertions.assertEquals(otherBefore.country(), v.country(),
            fmt + ": erasing identity 1 must not change identity 2's country");
        Assertions.assertEquals(otherBefore.telephone(), v.telephone(),
            fmt + ": erasing identity 1 must not change identity 2's telephone");
      }
    }
    Assertions.assertEquals(6, matched, fmt + ": all six target-identity rows must decode");
    Assertions.assertEquals(1, control, fmt + ": exactly one control-identity row must decode");
  }

  private NestedView firstView(final List<Object> rows, final int uid) {
    for (final Object o : rows) {
      final NestedView v = decodeRow(o);
      if (v != null && v.userId() == uid) {
        return v;
      }
    }
    return Assertions.fail("no row found for userId=" + uid);
  }

  private NestedView decodeRow(final Object row) {
    final String[] cols = ((String) row).split("\\t");
    if (cols.length != 3) {
      return null;
    }
    final String hexBody = cols[2];
    if (internalFormat == ColumnInternalFormat.AVRO) {
      final Msg3 m =
          (Msg3) AvroBodyConverter
              .convert(hexBody, Msg3.class);
      return new NestedView(m.getUserId(),
          String.valueOf(m.getBankDetails().getBankName()),
          String.valueOf(m.getBankDetails().getCardNum()),
          String.valueOf(m.getBankDetails().getPinCode()),
          String.valueOf(m.getCountry()),
          String.valueOf(m.getTelephone()));
    }
    final TestMessages.Msg3 m = ProtobufBodyConverter.convert(hexBody, TestMessages.Msg3.class);
    return new NestedView(m.getUserId(),
        m.getBankDetails().getBankName(),
        m.getBankDetails().getCardNum(),
        m.getBankDetails().getPinCode(),
        m.getCountry(),
        m.getTelephone());
  }

  private void provisionAndAttach(final String policy) throws CommandProcessorException, IOException {
    final Path policyFile = Files.createTempFile(policy + "_", ".erp");
    Files.write(policyFile, POLICY_DSL.getBytes());
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
