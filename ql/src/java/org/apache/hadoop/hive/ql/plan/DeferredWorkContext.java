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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;

import java.util.Collections;
import java.util.TreeMap;

/*
 * Context object that holds the information about the table and its replication context.
 */
public class DeferredWorkContext {
  public final boolean inReplScope;
  public final boolean replace;
  public final Long writeId;
  public final int stmtId;
  public final Hive hive;
  public final Context ctx;
  public final ImportTableDesc tblDesc;
  public final Path tgtPath;
  public Path destPath = null, loadPath = null;
  public LoadTableDesc.LoadFileType loadFileType;
  public boolean isSkipTrash;
  public boolean needRecycle;
  public boolean isCalculated = false;
  public Table table;

  public DeferredWorkContext(boolean replace, Path tgtPath, Long writeId, int stmtId, Hive hive, Context ctx,
                                ImportTableDesc tblDesc, boolean inReplScope) {
    this.replace = replace;
    this.writeId = writeId;
    this.stmtId = stmtId;
    this.hive = hive;
    this.ctx = ctx;
    this.tblDesc = tblDesc;
    this.inReplScope = inReplScope;
    this.tgtPath = tgtPath;
  }
  public boolean isCalculated() {
    return isCalculated;
  }
}
