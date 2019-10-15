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

package org.apache.hadoop.hive.ql.ddl.misc.hooks;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;

import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;

/**
 * Operation process of inserting a commit hook.
 */
public class InsertCommitHookOperation extends DDLOperation<InsertCommitHookDesc> {
  public InsertCommitHookOperation(DDLOperationContext context, InsertCommitHookDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws MetaException {
    HiveMetaHook hook = desc.getTable().getStorageHandler().getMetaHook();
    if (hook == null || !(hook instanceof DefaultHiveMetaHook)) {
      return 0;
    }
    DefaultHiveMetaHook hiveMetaHook = (DefaultHiveMetaHook) hook;

    try {
      hiveMetaHook.commitInsertTable(desc.getTable().getTTable(), desc.isOverwrite());
    } catch (Throwable t) {
      hiveMetaHook.rollbackInsertTable(desc.getTable().getTTable(), desc.isOverwrite());
      throw t;
    }

    return 0;
  }
}
