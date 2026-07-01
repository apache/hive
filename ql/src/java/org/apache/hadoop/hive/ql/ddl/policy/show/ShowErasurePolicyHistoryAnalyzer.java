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
package org.apache.hadoop.hive.ql.ddl.policy.show;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.anon.utils.PolicyPrivilegeUtils;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.PRIV_POLICY_VALIDATE;

/**
 * Analyzer for {@code SHOW ERASURE POLICY <name> HISTORY}. Reads the
 * lifecycle-event journal for the named policy through the metastore
 * client and writes one row per transition, ordered by transition time.
 *
 * <p>Gated on {@code POLICY_VALIDATE} on the named policy because the
 * history surface is part of the read/inspect tier of §4.5.
 */
@DDLType(types = HiveParser.TOK_SHOW_ERASURE_POLICY_HISTORY)
public class ShowErasurePolicyHistoryAnalyzer extends BaseSemanticAnalyzer {

  public ShowErasurePolicyHistoryAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() < 1) {
      throw new SemanticException("SHOW ERASURE POLICY HISTORY: missing policy name.");
    }
    final String policyName = root.getChild(0).getText();

    try {
      PolicyPrivilegeUtils.requirePrivilege(conf, Hive.get(conf),
          PRIV_POLICY_VALIDATE, policyName);
    } catch (org.apache.hadoop.hive.ql.metadata.HiveException he) {
      throw new SemanticException("SHOW ERASURE POLICY HISTORY: privilege check failed: "
          + he.getMessage());
    }

    ctx.setResFile(ctx.getLocalTmpPath());

    final ShowErasurePolicyHistoryDesc desc =
        new ShowErasurePolicyHistoryDesc(ctx.getResFile(), policyName);
    final Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);
    task.setFetchSource(true);
    setFetchTask(createFetchTask(ShowErasurePolicyHistoryDesc.SHOW_ERASURE_POLICY_HISTORY_SCHEMA));
  }
}
