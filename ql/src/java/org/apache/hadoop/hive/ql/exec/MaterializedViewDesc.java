/**
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
package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;

@Explain(displayName = "Materialized View Work", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class MaterializedViewDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String viewName;
  private final boolean retrieveAndInclude;
  private final boolean disableRewrite;
  private final boolean updateCreationMetadata;

  public MaterializedViewDesc(String viewName, boolean retrieveAndInclude, boolean disableRewrite,
      boolean updateCreationMetadata) {
    this.viewName = viewName;
    this.retrieveAndInclude = retrieveAndInclude;
    this.disableRewrite = disableRewrite;
    this.updateCreationMetadata = updateCreationMetadata;
  }

  public String getViewName() {
    return viewName;
  }

  public boolean isRetrieveAndInclude() {
    return retrieveAndInclude;
  }

  public boolean isDisableRewrite() {
    return disableRewrite;
  }

  public boolean isUpdateCreationMetadata() {
    return updateCreationMetadata;
  }
}
