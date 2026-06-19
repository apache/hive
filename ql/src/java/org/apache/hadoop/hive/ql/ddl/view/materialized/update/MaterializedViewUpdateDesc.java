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
package org.apache.hadoop.hive.ql.ddl.view.materialized.update;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;

/**
 * DDL task description of updating a materialized view.
 */
@Explain(displayName = "Materialized View Update", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class MaterializedViewUpdateDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final boolean retrieveAndInclude;
  private final boolean disableRewrite;
  private final boolean updateCreationMetadata;

  public MaterializedViewUpdateDesc(String name, boolean retrieveAndInclude, boolean disableRewrite,
      boolean updateCreationMetadata) {
    this.name = name;
    this.retrieveAndInclude = retrieveAndInclude;
    this.disableRewrite = disableRewrite;
    this.updateCreationMetadata = updateCreationMetadata;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getName() {
    return name;
  }

  @Explain(displayName = "retrieve and include", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isRetrieveAndInclude() {
    return retrieveAndInclude;
  }

  @Explain(displayName = "disable rewrite", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isDisableRewrite() {
    return disableRewrite;
  }

  @Explain(displayName = "update creation metadata", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isUpdateCreationMetadata() {
    return updateCreationMetadata;
  }
}
