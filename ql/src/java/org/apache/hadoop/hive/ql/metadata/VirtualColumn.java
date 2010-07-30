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

package org.apache.hadoop.hive.ql.metadata;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class VirtualColumn implements Serializable {

  private static final long serialVersionUID = 1L;

  public static HashMap<String, VirtualColumn> registry = new HashMap<String, VirtualColumn>();
  
  public static VirtualColumn FILENAME = new VirtualColumn("INPUT__FILE__NAME", (PrimitiveTypeInfo)TypeInfoFactory.stringTypeInfo);
  public static VirtualColumn BLOCKOFFSET = new VirtualColumn("BLOCK__OFFSET__INSIDE__FILE", (PrimitiveTypeInfo)TypeInfoFactory.longTypeInfo);
  
  static {
    registry.put(FILENAME.name, FILENAME);
    registry.put(BLOCKOFFSET.name, BLOCKOFFSET);
  }
  
  private String name;
  private PrimitiveTypeInfo typeInfo;
  private boolean isHidden = true;

  public VirtualColumn() {
  }
  
  public VirtualColumn(String name, PrimitiveTypeInfo typeInfo) {
    this(name, typeInfo, true);
  }
  
  VirtualColumn(String name, PrimitiveTypeInfo typeInfo, boolean isHidden) {
    this.name = name;
    this.typeInfo = typeInfo;
    this.isHidden = isHidden;
  }
  
  public PrimitiveTypeInfo getTypeInfo() {
    return typeInfo;
  }

  public void setTypeInfo(PrimitiveTypeInfo typeInfo) {
    this.typeInfo = typeInfo;
  }

  public String getName() {
    return this.name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public boolean isHidden() {
    return isHidden;
  }
  
  public boolean getIsHidden() {
    return isHidden;
  }

  public void setIsHidden(boolean isHidden) {
    this.isHidden = isHidden;
  }
  
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    VirtualColumn c = (VirtualColumn) o;
    return this.name.equals(c.name)
        && this.typeInfo.getTypeName().equals(c.getTypeInfo().getTypeName());
  }

}