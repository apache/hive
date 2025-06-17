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

package org.apache.hadoop.hive.ql.ddl.catalog.drop;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;

import java.io.Serializable;

/**
 * DDL task description for DROP CATALOG commands.
 */
@Explain(displayName = "Drop Catalog", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
public class DropCatalogDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String catalogName;
  private final boolean ifExists;

  public DropCatalogDesc(String catalogName, boolean ifExists) {
    this.catalogName = catalogName;
    this.ifExists = ifExists;
  }

  @Explain(displayName = "catalog", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
  public String getCatalogName() {
    return catalogName;
  }

  @Explain(displayName = "if exists")
  public boolean getIfExists() {
    return ifExists;
  }
}
