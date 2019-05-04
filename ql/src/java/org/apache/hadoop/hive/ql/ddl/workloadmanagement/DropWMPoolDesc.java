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
package org.apache.hadoop.hive.ql.ddl.workloadmanagement;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLTask2;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for DROP POOL commands.
 */
@Explain(displayName="Drop WM Pool", explainLevels={ Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DropWMPoolDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = -2608462103392563252L;

  static {
    DDLTask2.registerOperation(DropWMPoolDesc.class, DropWMPoolOperation.class);
  }

  private final String planName;
  private final String poolPath;

  public DropWMPoolDesc(String planName, String poolPath) {
    this.planName = planName;
    this.poolPath = poolPath;
  }

  @Explain(displayName="poolName", explainLevels={ Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPlanName() {
    return planName;
  }

  public String getPoolPath() {
    return poolPath;
  }
}
