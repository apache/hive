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
 */
package org.apache.hadoop.hive.ql.anon.cmd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyVersion;
import org.apache.hadoop.hive.metastore.api.PolicyVersionStatus;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestPolicyChecksumGate extends BaseTest {

  private static final String POLICY_OK     = "tpcg_ok";
  private static final String POLICY_TAMPER = "tpcg_tamper";
  private static final String POLICY_GONE   = "tpcg_gone";

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POLICY_OK, POLICY_TAMPER, POLICY_GONE };
  }

  private static final String POLICY_DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE telephone
      """;

  private static final String POLICY_DSL_ALT =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE ipList
      """;


  @Test
  @Order(1)
  public void loadStoresSourceAndValidateSucceeds()
      throws IOException, CommandProcessorException, HiveException {
    final Path path = Files.createTempFile("tpcg_ok_", ".erp");
    Files.write(path, POLICY_DSL.getBytes());

    execute("LOAD ERASURE POLICY " + POLICY_OK + " FROM '" + path + "'");

    final ErasurePolicyVersion draft = latestVersion(POLICY_OK);
    Assertions.assertNotNull(draft, "LOAD must persist a DRAFT row");
    Assertions.assertEquals(POLICY_DSL, draft.getSourceText(),
        "LOAD must persist the source text on the DRAFT row");
    Assertions.assertNotNull(draft.getSourceChecksum(),
        "LOAD must record the source checksum on the DRAFT row");
    Assertions.assertEquals(64, draft.getSourceChecksum().length(),
        "checksum must be a 64-char hex SHA-256");

    execute("VALIDATE ERASURE POLICY " + POLICY_OK);
    final ErasurePolicyVersion validated = latestVersion(POLICY_OK);
    Assertions.assertEquals(draft.getVersionId(), validated.getVersionId(),
        "VALIDATE must promote the same row");
    Assertions.assertEquals(
        PolicyVersionStatus.VALIDATED,
        validated.getStatus(),
        "the row must be VALIDATED after VALIDATE");
  }


  @Test
  @Order(2)
  public void fileEditedAfterLoadIsIgnored()
      throws IOException, CommandProcessorException, HiveException {
    final Path path = Files.createTempFile("tpcg_tamper_", ".erp");
    Files.write(path, POLICY_DSL.getBytes());

    execute("LOAD ERASURE POLICY " + POLICY_TAMPER + " FROM '" + path + "'");
    final ErasurePolicyVersion draft = latestVersion(POLICY_TAMPER);

    Files.write(path, POLICY_DSL_ALT.getBytes());

    execute("VALIDATE ERASURE POLICY " + POLICY_TAMPER);

    final ErasurePolicyVersion validated = latestVersion(POLICY_TAMPER);
    Assertions.assertEquals(draft.getVersionId(), validated.getVersionId(),
        "VALIDATE must promote the same row, ignoring the on-disk edit");
    Assertions.assertEquals(
        PolicyVersionStatus.VALIDATED,
        validated.getStatus(),
        "VALIDATE must succeed against the metastore-stored source despite the file edit");
    Assertions.assertEquals(POLICY_DSL, validated.getSourceText(),
        "VALIDATE must validate the ORIGINAL stored source, not the substituted file content");
  }

  @Test
  @Order(3)
  public void fileDeletedAfterLoadStillValidates()
      throws IOException, CommandProcessorException, HiveException {
    final Path path = Files.createTempFile("tpcg_gone_", ".erp");
    Files.write(path, POLICY_DSL.getBytes());

    execute("LOAD ERASURE POLICY " + POLICY_GONE + " FROM '" + path + "'");

    Files.delete(path);

    execute("VALIDATE ERASURE POLICY " + POLICY_GONE);
    final ErasurePolicyVersion validated = latestVersion(POLICY_GONE);
    Assertions.assertEquals(
        PolicyVersionStatus.VALIDATED,
        validated.getStatus(),
        "VALIDATE must succeed off the stored source even when the file is gone");
  }


  private ErasurePolicyVersion latestVersion(String policyName) throws HiveException {
    final List<ErasurePolicyVersion> all = hive.listErasurePolicyVersions(policyName);
    if (all == null || all.isEmpty()) {
      return null;
    }
    ErasurePolicyVersion latest = all.get(0);
    for (final ErasurePolicyVersion v : all) {
      if (v.getVersionId() > latest.getVersionId()) {
        latest = v;
      }
    }
    return latest;
  }
}
