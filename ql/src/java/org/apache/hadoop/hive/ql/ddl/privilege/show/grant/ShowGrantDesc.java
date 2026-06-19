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

package org.apache.hadoop.hive.ql.ddl.privilege.show.grant;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for SHOW GRANT commands.
 */
@Explain(displayName="Show grant desc", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowGrantDesc implements DDLDesc {
  public static final String SCHEMA =
      "database,table,partition,column,principal_name,principal_type,privilege," +
      "grant_option,grant_time,grantor#" +
      "string:string:string:string:string:string:string:boolean:bigint:string";

  private final String resFile;
  private final PrincipalDesc principal;
  private final PrivilegeObjectDesc hiveObj;

  public ShowGrantDesc(String resFile, PrincipalDesc principal, PrivilegeObjectDesc hiveObj) {
    this.resFile = resFile;
    this.principal = principal;
    this.hiveObj = hiveObj;
  }

  @Explain(displayName="principal desc", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public PrincipalDesc getPrincipalDesc() {
    return principal;
  }

  @Explain(skipHeader = true,  explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public PrivilegeObjectDesc getHiveObj() {
    return hiveObj;
  }

  public String getResFile() {
    return resFile;
  }
}
