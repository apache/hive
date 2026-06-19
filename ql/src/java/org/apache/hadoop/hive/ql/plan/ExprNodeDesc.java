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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * ExprNodeDesc.
 *
 */
public abstract class ExprNodeDesc implements Serializable, Node {
  private static final long serialVersionUID = 1L;
  protected TypeInfo typeInfo;

  public ExprNodeDesc() {
  }

  public ExprNodeDesc(TypeInfo typeInfo) {
    this.typeInfo = typeInfo;
    if (typeInfo == null) {
      throw new RuntimeException("typeInfo cannot be null!");
    }
  }

  @Override
  public abstract ExprNodeDesc clone();

  // Cant use equals because the walker depends on them being object equal
  // The default graph walker processes a node after its kids have been
  // processed. That comparison needs
  // object equality - isSame means that the objects are semantically equal.
  public abstract boolean isSame(Object o);

  @Override
  public int hashCode() {
    return typeInfo.hashCode();
  }

  @Override
  public final boolean equals(Object o) {
    // prevent equals from being overridden in sub-classes
    // always use ExprNodeDescEqualityWrapper
    // if you need any other equality than Object.equals()
    return (o == this);
  }

  public TypeInfo getTypeInfo() {
    return typeInfo;
  }

  public void setTypeInfo(TypeInfo typeInfo) {
    this.typeInfo = typeInfo;
  }

  public String getExprString() {
    assert (false);
    return null;
  }

  @Explain(jsonOnly = true)
  public String getExprStringForExplain() {
    return toString();
  }

  public String getExprString(boolean sortChildren) {
    return getExprString();
  }

  public ObjectInspector getWritableObjectInspector() {
    return TypeInfoUtils
      .getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
  }

  public String getTypeString() {
    return typeInfo.getTypeName();
  }

  public List<String> getCols() {
    return null;
  }

  @Override
  public List<ExprNodeDesc> getChildren() {
    return null;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  // This wraps an instance of an ExprNodeDesc, and makes equals work like isSame, see comment on
  // isSame
  public final static class ExprNodeDescEqualityWrapper {
    private final ExprNodeDesc exprNodeDesc;
    // beware of any implementation whose hashcode is mutable by reference
    // inserting into a Map and then changing the hashcode can make it 
    // disappear out of the Map during lookups
    private final int hashcode;

    public ExprNodeDescEqualityWrapper(ExprNodeDesc exprNodeDesc) {
      this.exprNodeDesc = exprNodeDesc;
      this.hashcode = exprNodeDesc == null ? 0 : exprNodeDesc.hashCode();
    }

    public ExprNodeDesc getExprNodeDesc() {
      return exprNodeDesc;
    }

    @Override
    public boolean equals(Object other) {

      if (other == null || !(other instanceof ExprNodeDescEqualityWrapper)) {
        return false;
      }

      return this.exprNodeDesc.isSame(((ExprNodeDescEqualityWrapper)other).getExprNodeDesc());
    }

    @Override
    public int hashCode() {
      return hashcode;
    }

    /* helper function to allow Set()/Collection() operations with ExprNodeDesc */
    public static Collection<ExprNodeDescEqualityWrapper> transform(
        Collection<ExprNodeDesc> descs) {
      if (descs == null) {
        return null;
      }
      final Collection<ExprNodeDescEqualityWrapper> wrapped = new ArrayList<ExprNodeDesc.ExprNodeDescEqualityWrapper>(
          descs.size());
      for (ExprNodeDesc desc : descs) {
        wrapped.add(new ExprNodeDescEqualityWrapper(desc));
      }
      return wrapped;
    }
  }

}
