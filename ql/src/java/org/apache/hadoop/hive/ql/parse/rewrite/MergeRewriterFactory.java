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
package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;

import static org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory.TARGET_PREFIX;
import static org.apache.commons.lang3.StringUtils.EMPTY;

public class MergeRewriterFactory implements RewriterFactory<MergeStatement> {
  private final Hive db;
  private final HiveConf conf;

  public MergeRewriterFactory(HiveConf conf) throws SemanticException {
    try {
      this.db = Hive.get(conf);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    this.conf = conf;
  }

  public Rewriter<MergeStatement> createRewriter(Table table, String targetTableFullName, String subQueryAlias)
      throws SemanticException {
    boolean splitUpdate = HiveConf.getBoolVar(conf, HiveConf.ConfVars.SPLIT_UPDATE);
    boolean copyOnWriteMode = false;
    HiveStorageHandler storageHandler = table.getStorageHandler();
    if (storageHandler != null) {
      copyOnWriteMode = storageHandler.shouldOverwrite(table, Context.Operation.MERGE);
    }
    
    SqlGeneratorFactory sqlGeneratorFactory = new SqlGeneratorFactory(
        table, targetTableFullName, conf, !copyOnWriteMode ? subQueryAlias : null, 
        copyOnWriteMode ? TARGET_PREFIX : EMPTY);

    if (copyOnWriteMode) {
      return new CopyOnWriteMergeRewriter(db, conf, sqlGeneratorFactory);
    } else if (splitUpdate) {
      return new SplitMergeRewriter(db, conf, sqlGeneratorFactory);
    } else {
      if (AcidUtils.isNonNativeAcidTable(table)) {
        throw new SemanticException(ErrorMsg.NON_NATIVE_ACID_UPDATE.getErrorCodedMsg());
      }
      return new MergeRewriter(db, conf, sqlGeneratorFactory);
    }
  }
}
