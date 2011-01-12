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
package org.apache.hadoop.hive.ql.plan;

import java.util.List;

@Explain(displayName="show grant desc")
public class ShowGrantDesc {
  
  private PrincipalDesc principalDesc;

  private PrivilegeObjectDesc hiveObj;
  
  private List<String> columns;

  private String resFile;

  public ShowGrantDesc(){
  }
  
  public ShowGrantDesc(String resFile, PrincipalDesc principalDesc,
      PrivilegeObjectDesc subjectObj, List<String> columns) {
    this.resFile = resFile;
    this.principalDesc = principalDesc;
    this.hiveObj = subjectObj;
    this.columns = columns;
  }
  
  @Explain(displayName="principal desc")
  public PrincipalDesc getPrincipalDesc() {
    return principalDesc;
  }

  public void setPrincipalDesc(PrincipalDesc principalDesc) {
    this.principalDesc = principalDesc;
  }

  @Explain(displayName="object")
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
  
  public List<String> getColumns() {
    return columns;
  }

  public void setColumns(List<String> columns) {
    this.columns = columns;
  }
  
}
