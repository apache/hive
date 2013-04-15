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

package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.ArrayList;
import java.util.List;

public class DelegatedUnionObjectInspector implements UnionObjectInspector {

  private UnionObjectInspector delegate;
  private List<ObjectInspector> children;

  public DelegatedUnionObjectInspector(UnionObjectInspector delegate) {
    this.delegate = delegate;
  }

  public void reset(UnionObjectInspector current) {
    this.delegate = current;
    if (children != null) {
      int index = 0;
      List<ObjectInspector> newOIs = delegate.getObjectInspectors();
      for (ObjectInspector child : children) {
        DelegatedObjectInspectorFactory.reset(child, newOIs.get(index++));
      }
    }
  }

  public List<ObjectInspector> getObjectInspectors() {
    if (children != null || delegate.getObjectInspectors() == null) {
      return children;
    }
    List<ObjectInspector> inspectors = delegate.getObjectInspectors();
    List<ObjectInspector> delegated = new ArrayList<ObjectInspector>();
    for (ObjectInspector inspector : inspectors) {
      delegated.add(DelegatedObjectInspectorFactory.wrap(inspector));
    }
    return children = delegated;
  }

  public byte getTag(Object o) {
    return delegate.getTag(o);
  }

  public Object getField(Object o) {
    return delegate.getField(o);
  }

  public String getTypeName() {
    return delegate.getTypeName();
  }

  public Category getCategory() {
    return delegate.getCategory();
  }
}
