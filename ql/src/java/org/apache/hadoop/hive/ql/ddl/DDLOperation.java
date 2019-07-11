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

package org.apache.hadoop.hive.ql.ddl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract ancestor class of all DDL Operation classes.
 *
 * A class that is extending this abstract class, and which is under the org.apache.hadoop.hive.ql.ddl package
 * will be registered automatically as the operation for it's generic DDLDesc argument.
 */
public abstract class DDLOperation<T extends DDLDesc> {
  protected static final Logger LOG = LoggerFactory.getLogger("hive.ql.exec.DDLTask");

  protected final DDLOperationContext context;
  protected final T desc;

  public DDLOperation(DDLOperationContext context, T desc) {
    this.context = context;
    this.desc = desc;
  }

  public abstract int execute() throws Exception;
}
