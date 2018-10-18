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

import java.io.Serializable;

import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Create/Alter Mapping",
    explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateOrAlterWMMappingDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = -442968568922083053L;

  private WMMapping mapping;
  private boolean update;

  public CreateOrAlterWMMappingDesc() {}

  public CreateOrAlterWMMappingDesc(WMMapping mapping, boolean update) {
    this.mapping = mapping;
    this.update = update;
  }

  @Explain(displayName = "mapping", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public WMMapping getMapping() {
    return mapping;
  }

  public void setMapping(WMMapping mapping) {
    this.mapping = mapping;
  }

  @Explain(displayName = "update",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isUpdate() {
    return update;
  }

  public void setUpdate(boolean update) {
    this.update = update;
  }
}
