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

import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Create/Alter Pool", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateOrAlterWMPoolDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 4872940135771213510L;

  private WMPool createPool;
  private WMNullablePool alterPool;
  private String poolPath;
  private boolean update;

  public CreateOrAlterWMPoolDesc() {}

  public CreateOrAlterWMPoolDesc(WMPool pool, String poolPath, boolean update) {
    this.createPool = pool;
    this.poolPath = poolPath;
    this.update = update;
  }

  public CreateOrAlterWMPoolDesc(WMNullablePool pool, String poolPath, boolean update) {
    this.alterPool = pool;
    this.poolPath = poolPath;
    this.update = update;
  }

  @Explain(displayName="pool", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Object getPool() {
    return createPool == null ? alterPool : createPool;
  }

  public WMPool getCreatePool() {
    return createPool;
  }

  public WMNullablePool getAlterPool() {
    return alterPool;
  }

  public void setCreatePool(WMPool pool) {
    this.createPool = pool;
  }

  public void setAlterPool(WMNullablePool pool) {
    this.alterPool = pool;
  }

  @Explain(displayName="poolPath", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPoolPath() {
    return poolPath;
  }

  public void setPoolPath(String poolPath) {
    this.poolPath = poolPath;
  }

  @Explain(displayName="isUpdate", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isUpdate() {
    return update;
  }

  public void setUpdate(boolean update) {
    this.update = update;
  }
}
