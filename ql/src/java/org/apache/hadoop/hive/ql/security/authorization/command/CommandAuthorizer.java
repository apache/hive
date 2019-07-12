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

package org.apache.hadoop.hive.ql.security.authorization.command;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.google.common.collect.Sets;

/**
 * Does authorization using post semantic analysis information from the semantic analyzer.
 */
public final class CommandAuthorizer {
  private CommandAuthorizer() {
    throw new UnsupportedOperationException("CommandAuthorizer should not be instantiated");
  }

  /** @param command Passed so that authorization interface can provide more useful information in logs. */
  public static void doAuthorization(HiveOperation op, BaseSemanticAnalyzer sem, String command)
      throws HiveException, AuthorizationException {
    if (skip(op, sem)) {
      return;
    }

    SessionState ss = SessionState.get();

    Set<ReadEntity> inputs = getInputs(sem);
    Set<WriteEntity> outputs = getOutputs(sem);

    if (!ss.isAuthorizationModeV2()) {
      CommandAuthorizerV1.doAuthorization(op, sem, ss, inputs, outputs);
    } else {
      CommandAuthorizerV2.doAuthorization(op, sem, ss, inputs, outputs, command);
    }
  }

  private static boolean skip(HiveOperation op, BaseSemanticAnalyzer sem) throws HiveException {
    // skipping the auth check for the "CREATE DATABASE" operation if database already exists
    // we know that if the database already exists then "CREATE DATABASE" operation will fail.
    if (op == HiveOperation.CREATEDATABASE) {
      for (WriteEntity e : sem.getOutputs()) {
        if(e.getType() == Entity.Type.DATABASE && sem.getDb().databaseExists(e.getName().split(":")[1])){
          return true;
        }
      }
    }

    return false;
  }

  private static Set<ReadEntity> getInputs(BaseSemanticAnalyzer sem) {
    Set<ReadEntity> additionalInputs = new HashSet<ReadEntity>();
    for (Entity e : sem.getInputs()) {
      if (e.getType() == Entity.Type.PARTITION) {
        additionalInputs.add(new ReadEntity(e.getTable()));
      }
    }

    // Sets.union keeps the values from the first set if they are present in both
    return Sets.union(sem.getInputs(), additionalInputs);
  }

  private static Set<WriteEntity> getOutputs(BaseSemanticAnalyzer sem) {
    Set<WriteEntity> additionalOutputs = new HashSet<WriteEntity>();
    for (WriteEntity e : sem.getOutputs()) {
      if (e.getType() == Entity.Type.PARTITION) {
        additionalOutputs.add(new WriteEntity(e.getTable(), e.getWriteType()));
      }
    }

    // Sets.union keeps the values from the first set if they are present in both
    return Sets.union(sem.getOutputs(), additionalOutputs);
  }
}
