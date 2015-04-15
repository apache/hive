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

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * CreateFunctionDesc.
 *
 */
@Explain(displayName = "Create Function", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateFunctionDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private String functionName;
  private String className;
  private boolean isTemp;
  private List<ResourceUri> resources;

  /**
   * For serialization only.
   */
  public CreateFunctionDesc() {
  }
  
  public CreateFunctionDesc(String functionName, boolean isTemp, String className,
      List<ResourceUri> resources) {
    this.functionName = functionName;
    this.isTemp = isTemp;
    this.className = className;
    this.resources = resources;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getFunctionName() {
    return functionName;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  @Explain(displayName = "class")
  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public boolean isTemp() {
    return isTemp;
  }

  public void setTemp(boolean isTemp) {
    this.isTemp = isTemp;
  }

  public List<ResourceUri> getResources() {
    return resources;
  }

  public void setResources(List<ResourceUri> resources) {
    this.resources = resources;
  }

}
