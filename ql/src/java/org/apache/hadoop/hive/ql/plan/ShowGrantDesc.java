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
import org.apache.hadoop.hive.ql.plan.Explain.Level;


@Explain(displayName="show grant desc", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowGrantDesc {
  
  private PrincipalDesc principalDesc;

  private PrivilegeObjectDesc hiveObj;
  
  private String resFile;

  /**
   * thrift ddl for the result of show grant.
   */
  private static final String tabularSchema =
      "database,table,partition,column,principal_name,principal_type,privilege," +
      "grant_option,grant_time,grantor#" +
      "string:string:string:string:string:string:string:boolean:bigint:string";

  public ShowGrantDesc(){
  }
  
  public ShowGrantDesc(String resFile, PrincipalDesc principalDesc,
      PrivilegeObjectDesc subjectObj) {
    this.resFile = resFile;
    this.principalDesc = principalDesc;
    this.hiveObj = subjectObj;
  }

  public static String getSchema() {
    return tabularSchema;
  }

  @Explain(displayName="principal desc", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public PrincipalDesc getPrincipalDesc() {
    return principalDesc;
  }

  public void setPrincipalDesc(PrincipalDesc principalDesc) {
    this.principalDesc = principalDesc;
  }

  @Explain(displayName="object", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public PrivilegeObjectDesc getHiveObj() {
    return hiveObj;
  }

  public void setHiveObj(PrivilegeObjectDesc subjectObj) {
    this.hiveObj = subjectObj;
  }
  
  public String getResFile() {
    return resFile;
  }

  public void setResFile(String resFile) {
    this.resFile = resFile;
  }
}
