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

package org.apache.hadoop.hive.ql.ddl.privilege.show.rolegrant;

import java.io.Serializable;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for SHOW ROLE GRANT commands.
 */
@Explain(displayName = "Show Role Grant", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowRoleGrantDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  public static final String SCHEMA =
      "role,grant_option,grant_time,grantor#" +
      "string:boolean:bigint:string";

  private final String name;
  private final PrincipalType principalType;
  private final String resFile;

  public ShowRoleGrantDesc(String name, PrincipalType principalType, String resFile) {
    this.name = name;
    this.principalType = principalType;
    this.resFile = resFile;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getName() {
    return name;
  }

  public PrincipalType getPrincipalType() {
    return principalType;
  }

  public String getResFile() {
    return resFile;
  }
}
