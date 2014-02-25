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

package org.apache.hadoop.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import java.util.List;

public class MFunction {

  private String functionName;
  private MDatabase database;
  private String className;
  private String ownerName;
  private String ownerType;
  private int createTime;
  private int functionType;
  private List<MResourceUri> resourceUris;

  public MFunction() {
  }

  public MFunction(String functionName,
      MDatabase database,
      String className,
      String ownerName,
      String ownerType,
      int createTime,
      int functionType,
      List<MResourceUri> resourceUris) {
    this.setFunctionName(functionName);
    this.setDatabase(database);
    this.setFunctionType(functionType);
    this.setClassName(className);
    this.setOwnerName(ownerName);
    this.setOwnerType(ownerType);
    this.setCreateTime(createTime);
    this.setResourceUris(resourceUris);
  }

  public String getFunctionName() {
    return functionName;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public MDatabase getDatabase() {
    return database;
  }

  public void setDatabase(MDatabase database) {
    this.database = database;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public void setOwnerName(String owner) {
    this.ownerName = owner;
  }

  public String getOwnerType() {
    return ownerType;
  }

  public void setOwnerType(String ownerType) {
    this.ownerType = ownerType;
  }

  public int getCreateTime() {
    return createTime;
  }

  public void setCreateTime(int createTime) {
    this.createTime = createTime;
  }

  public int getFunctionType() {
    return functionType;
  }

  public void setFunctionType(int functionType) {
    this.functionType = functionType;
  }

  public List<MResourceUri> getResourceUris() {
    return resourceUris;
  }

  public void setResourceUris(List<MResourceUri> resourceUris) {
    this.resourceUris = resourceUris;
  }  
}
