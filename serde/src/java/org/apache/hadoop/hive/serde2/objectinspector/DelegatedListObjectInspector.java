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

import java.util.List;

public class DelegatedListObjectInspector implements ListObjectInspector {

  private ListObjectInspector delegate;
  private ObjectInspector element;

  protected DelegatedListObjectInspector() {
    super();
  }
  public DelegatedListObjectInspector(ListObjectInspector delegate) {
    this.delegate = delegate;
  }

  public void reset(ListObjectInspector delegate) {
    this.delegate = delegate;
    if (element != null) {
      DelegatedObjectInspectorFactory.reset(element, delegate.getListElementObjectInspector());
    }
  }

  public ObjectInspector getListElementObjectInspector() {
    return element != null ? element :
        (element = DelegatedObjectInspectorFactory.wrap(delegate.getListElementObjectInspector()));
  }

  public Object getListElement(Object data, int index) {
    return delegate.getListElement(data, index);
  }

  public int getListLength(Object data) {
    return delegate.getListLength(data);
  }

  public List<?> getList(Object data) {
    return delegate.getList(data);
  }

  public String getTypeName() {
    return delegate.getTypeName();
  }

  public Category getCategory() {
    return delegate.getCategory();
  }
}
