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

public class DelegatedStructObjectInspector extends StructObjectInspector {

  private StructObjectInspector delegate;
  private List<DelegatedStructField> fields;

  public DelegatedStructObjectInspector(StructObjectInspector delegate) {
    this.delegate = delegate;
  }

  public void reset(StructObjectInspector current) {
    this.delegate = current;
    if (fields != null) {
      int index = 0;
      List<? extends StructField> newFields = delegate.getAllStructFieldRefs();
      for (DelegatedStructField field : fields) {
        field.field = newFields.get(index++);
      }
    }
  }

  private static class DelegatedStructField implements StructField {
    private StructField field;

    public DelegatedStructField(StructField field) {
      this.field = field;
    }
    public String getFieldName() {
      return field.getFieldName();
    }
    public ObjectInspector getFieldObjectInspector() {
      return field.getFieldObjectInspector();
    }
    public String getFieldComment() {
      return field.getFieldComment();
    }
  }

  public List<? extends StructField> getAllStructFieldRefs() {
    if (fields != null || delegate.getAllStructFieldRefs() == null) {
      return fields;
    }
    List<? extends StructField> fields = delegate.getAllStructFieldRefs();
    List<DelegatedStructField> delegate = new ArrayList<DelegatedStructField>(fields.size());
    for (StructField field : fields) {
      delegate.add(new DelegatedStructField(field));
    }
    return this.fields = delegate;
  }

  public StructField getStructFieldRef(String fieldName) {
    StructField field = delegate.getStructFieldRef(fieldName);
    return field == null ? null : new DelegatedStructField(field);
  }

  public Object getStructFieldData(Object data, StructField fieldRef) {
    return delegate.getStructFieldData(data, ((DelegatedStructField) fieldRef).field);
  }

  public List<Object> getStructFieldsDataAsList(Object data) {
    return delegate.getStructFieldsDataAsList(data);
  }

  public String getTypeName() {
    return delegate.getTypeName();
  }

  public Category getCategory() {
    return delegate.getCategory();
  }
}
