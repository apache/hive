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

package org.apache.hadoop.hive.ql.exec;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectsEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class KeyWrapperFactory {
  public KeyWrapperFactory(ExprNodeEvaluator[] keyFields, ObjectInspector[] keyObjectInspectors,
      ObjectInspector[] currentKeyObjectInspectors) {
    this.keyFields = keyFields;
    this.keyObjectInspectors = keyObjectInspectors;
    this.currentKeyObjectInspectors = currentKeyObjectInspectors;

  }

  public KeyWrapper getKeyWrapper() {
    if (keyFields.length == 1
        && TypeInfoUtils.getTypeInfoFromObjectInspector(keyObjectInspectors[0]).equals(
            TypeInfoFactory.stringTypeInfo)) {
      assert(TypeInfoUtils.getTypeInfoFromObjectInspector(currentKeyObjectInspectors[0]).equals(
            TypeInfoFactory.stringTypeInfo));
      soi_new = (StringObjectInspector) keyObjectInspectors[0];
      soi_copy = (StringObjectInspector) currentKeyObjectInspectors[0];
      return new TextKeyWrapper(false);
    } else {
      currentStructEqualComparer = new ListObjectsEqualComparer(currentKeyObjectInspectors, currentKeyObjectInspectors);
      newKeyStructEqualComparer = new ListObjectsEqualComparer(currentKeyObjectInspectors, keyObjectInspectors);
      return new ListKeyWrapper(false);
    }
  }

  transient ExprNodeEvaluator[] keyFields;
  transient ObjectInspector[] keyObjectInspectors;
  transient ObjectInspector[] currentKeyObjectInspectors;


  transient ListObjectsEqualComparer currentStructEqualComparer;
  transient ListObjectsEqualComparer newKeyStructEqualComparer;

  class ListKeyWrapper extends KeyWrapper {
    int hashcode;
    Object[] keys;
    // decide whether this is already in hashmap (keys in hashmap are deepcopied
    // version, and we need to use 'currentKeyObjectInspector').
    ListObjectsEqualComparer equalComparer;

    public ListKeyWrapper(boolean isCopy) {
      this(-1, new Object[keyFields.length], isCopy);
    }

    private ListKeyWrapper(int hashcode, Object[] copiedKeys,
        boolean isCopy) {
      super();
      this.hashcode = hashcode;
      keys = copiedKeys;
      setEqualComparer(isCopy);
    }

    private void setEqualComparer(boolean copy) {
      if (!copy) {
        equalComparer = newKeyStructEqualComparer;
      } else {
        equalComparer = currentStructEqualComparer;
      }
    }

    @Override
    public int hashCode() {
      return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
      Object[] copied_in_hashmap = ((ListKeyWrapper) obj).keys;
      return equalComparer.areEqual(copied_in_hashmap, keys);
    }

    @Override
    public void setHashKey() {
      hashcode = ObjectInspectorUtils.writableArrayHashCode(keys);
    }

    @Override
    public void getNewKey(Object row, ObjectInspector rowInspector) throws HiveException {
      // Compute the keys
      for (int i = 0; i < keyFields.length; i++) {
        keys[i]  = keyFields[i].evaluate(row);
      }
    }

    @Override
    public KeyWrapper copyKey() {
      Object[] newDefaultKeys = deepCopyElements(keys, keyObjectInspectors,
          ObjectInspectorCopyOption.WRITABLE);
      return new ListKeyWrapper(hashcode, newDefaultKeys, true);
    }

    @Override
    public void copyKey(KeyWrapper oldWrapper) {
      ListKeyWrapper listWrapper = (ListKeyWrapper) oldWrapper;
      hashcode = listWrapper.hashcode;
      equalComparer = currentStructEqualComparer;
      deepCopyElements(listWrapper.keys, keyObjectInspectors, keys,
          ObjectInspectorCopyOption.WRITABLE);
    }

    @Override
    public Object[] getKeyArray() {
      return keys;
    }

    private Object[] deepCopyElements(Object[] keys,
        ObjectInspector[] keyObjectInspectors,
        ObjectInspectorCopyOption copyOption) {
      Object[] result = new Object[keys.length];
      deepCopyElements(keys, keyObjectInspectors, result, copyOption);
      return result;
    }

    private void deepCopyElements(Object[] keys,
        ObjectInspector[] keyObjectInspectors, Object[] result,
        ObjectInspectorCopyOption copyOption) {
      for (int i = 0; i < keys.length; i++) {
        result[i] = ObjectInspectorUtils.copyToStandardObject(keys[i],
            keyObjectInspectors[i], copyOption);
      }
    }
  }

  transient Object[] singleEleArray = new Object[1];
  transient StringObjectInspector soi_new, soi_copy;

  class TextKeyWrapper extends KeyWrapper {
    int hashcode;
    Object key;
    boolean isCopy;

    public TextKeyWrapper(boolean isCopy) {
      this(-1, null, isCopy);
    }

    private TextKeyWrapper(int hashcode, Object key,
        boolean isCopy) {
      super();
      this.hashcode = hashcode;
      this.key = key;
      this.isCopy = isCopy;
    }

    @Override
    public int hashCode() {
      return hashcode;
    }

    @Override
    public boolean equals(Object other) {
      Object obj = ((TextKeyWrapper) other).key;
      Text t1;
      Text t2;
      if (isCopy) {
        t1 =  soi_copy.getPrimitiveWritableObject(key);
        t2 =  soi_copy.getPrimitiveWritableObject(obj);
      } else {
        t1 = soi_new.getPrimitiveWritableObject(key);
        t2 = soi_copy.getPrimitiveWritableObject(obj);
      }
      if (t1 == null && t2 == null) {
        return true;
      } else if (t1 == null || t2 == null) {
        return false;
      } else {
        return t1.equals(t2);
      }
    }

    @Override
    public void setHashKey() {
      if (key == null) {
        hashcode = 0;
      } else{
        hashcode = key.hashCode();
      }
    }

    @Override
    public void getNewKey(Object row, ObjectInspector rowInspector) throws HiveException {
      // Compute the keys
      key = keyFields[0].evaluate(row);
    }

    @Override
    public KeyWrapper copyKey() {
      return new TextKeyWrapper(hashcode, ObjectInspectorUtils.copyToStandardObject(key,
          soi_new, ObjectInspectorCopyOption.WRITABLE), true);
    }

    @Override
    public void copyKey(KeyWrapper oldWrapper) {
      TextKeyWrapper textWrapper = (TextKeyWrapper) oldWrapper;
      hashcode = textWrapper.hashcode;
      isCopy = true;
      key = ObjectInspectorUtils.copyToStandardObject(textWrapper.key,
          soi_new, ObjectInspectorCopyOption.WRITABLE);
    }

    @Override
    public Object[] getKeyArray() {
      singleEleArray[0] = key;
      return singleEleArray;
    }
  }
}
