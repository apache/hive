/**
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
package org.apache.hadoop.hive.ql.parse.authorization;

import java.io.Serializable;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * HiveAuthorizationTaskFactory creates DDL authorization related
 * tasks. Every method in this class may return null, indicating no task
 * needs to be executed or can throw a SemanticException.
 */
public interface HiveAuthorizationTaskFactory {
  public Task<? extends Serializable> createCreateRoleTask(ASTNode node, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException;

  public Task<? extends Serializable> createDropRoleTask(ASTNode node, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException;

  public Task<? extends Serializable> createShowRoleGrantTask(ASTNode node, Path resultFile,
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) throws SemanticException;

  public Task<? extends Serializable> createGrantRoleTask(ASTNode node, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException;

  public Task<? extends Serializable> createRevokeRoleTask(ASTNode node, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException;

  public Task<? extends Serializable> createGrantTask(ASTNode node, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException;

  public Task<? extends Serializable> createShowGrantTask(ASTNode node, Path resultFile, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException;

  public Task<? extends Serializable> createRevokeTask(ASTNode node, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException;
}
