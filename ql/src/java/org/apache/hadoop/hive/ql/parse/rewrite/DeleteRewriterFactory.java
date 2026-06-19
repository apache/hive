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
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;

import static org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory.DELETE_PREFIX;

public class DeleteRewriterFactory implements RewriterFactory<DeleteStatement> {
  protected final HiveConf conf;

  public DeleteRewriterFactory(HiveConf conf) {
    this.conf = conf;
  }

  public Rewriter<DeleteStatement> createRewriter(Table table, String targetTableFullName, String subQueryAlias) {
    boolean copyOnWriteMode = false;
    HiveStorageHandler storageHandler = table.getStorageHandler();
    if (storageHandler != null) {
      copyOnWriteMode = storageHandler.shouldOverwrite(table, Context.Operation.DELETE);
    }

    SqlGeneratorFactory sqlGeneratorFactory = new SqlGeneratorFactory(
        table, targetTableFullName, conf, null, DELETE_PREFIX);

    if (copyOnWriteMode) {
      return new CopyOnWriteDeleteRewriter(conf, sqlGeneratorFactory);
    } else {
      return new DeleteRewriter(sqlGeneratorFactory);
    }
  }
}
