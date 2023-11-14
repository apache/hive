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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;

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

  @Override
  public Rewriter<MergeStatement> createRewriter(Table table, String targetTableFullName, String subQueryAlias)
      throws SemanticException {
    boolean splitUpdate = HiveConf.getBoolVar(conf, HiveConf.ConfVars.SPLIT_UPDATE);
    boolean nonNativeAcid = AcidUtils.isNonNativeAcidTable(table, true);
    if (nonNativeAcid && !splitUpdate) {
      throw new SemanticException(ErrorMsg.NON_NATIVE_ACID_UPDATE.getErrorCodedMsg());
    }

    SqlGeneratorFactory sqlGeneratorFactory = new SqlGeneratorFactory(
        table,
        targetTableFullName,
        conf,
        subQueryAlias,
        StringUtils.EMPTY);

    if (splitUpdate) {
      return new SplitMergeRewriter(db, conf, sqlGeneratorFactory);
    }
    return new MergeRewriter(db, conf, sqlGeneratorFactory);
  }
}
