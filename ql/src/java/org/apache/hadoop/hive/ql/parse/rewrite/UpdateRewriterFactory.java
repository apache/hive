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
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;

import static org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory.DELETE_PREFIX;
import static org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory.SUB_QUERY_ALIAS;

public class UpdateRewriterFactory implements RewriterFactory<UpdateStatement> {
  protected final HiveConf conf;

  public UpdateRewriterFactory(HiveConf conf) {
    this.conf = conf;
  }

  public Rewriter<UpdateStatement> createRewriter(Table table, String targetTableFullName, String subQueryAlias)
      throws SemanticException {
    boolean splitUpdate = HiveConf.getBoolVar(conf, HiveConf.ConfVars.SPLIT_UPDATE);
    boolean copyOnWriteMode = false;
    HiveStorageHandler storageHandler = table.getStorageHandler();
    if (storageHandler != null) {
      copyOnWriteMode = storageHandler.shouldOverwrite(table, Context.Operation.UPDATE);
    }

    SqlGeneratorFactory sqlGeneratorFactory = new SqlGeneratorFactory(
        table, targetTableFullName, conf, splitUpdate && !copyOnWriteMode ? SUB_QUERY_ALIAS : null, DELETE_PREFIX);

    if (copyOnWriteMode) {
      return new CopyOnWriteUpdateRewriter(conf, sqlGeneratorFactory);
    } else if (splitUpdate) {
      return new SplitUpdateRewriter(conf, sqlGeneratorFactory);
    } else {
      if (AcidUtils.isNonNativeAcidTable(table)) {
        throw new SemanticException(ErrorMsg.NON_NATIVE_ACID_UPDATE.getErrorCodedMsg());
      }
      return new UpdateRewriter(conf, sqlGeneratorFactory);
    }
  }
}
