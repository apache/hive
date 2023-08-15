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

package org.apache.hadoop.hive.ql.parse;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.util.NullOrdering;

/**
 * ReadOnlySemanticAnalyzer serves as an interface that can be used to pass into classes
 * that need access to the SemanticAnalyzer but only need certain operations that will
 * be read only.
 *
 * This is for semantic purposes. Someone can still mutate the underlying object as with
 * any dereference pointer. For instance, a user can get a Context and manipulate the
 * contents of the Context since that is a mutable object. Ultimately, this should be fixed
 * by making all the objects immutable that are returned by this interface.
 */
public interface ReadOnlySemanticAnalyzer {

  public Context getContext();

  public Hive getDb();

  public HiveConf getConf();

  public boolean isCBOExecuted();

  public HiveTxnManager getTxnMgr();

  public SemanticAnalyzer.MaterializationRebuildMode getMVRebuildMode();

  public int getDestTableId();

  public Set<ReadEntity> getInputs();

  public Set<WriteEntity> getOutputs();

  public boolean enableColumnStatsCollecting();

  public boolean allowOutputMultipleTimes();

  public NullOrdering getDefaultNullOrdering();

  public boolean isResultsCacheEnabled();

  public boolean queryTypeCanUseCache();

  public UnparseTranslator getUnparseTranslator();

  public boolean isCalcitePlanner();
}
