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

package org.apache.hadoop.hive.ql.exec;

import java.util.Comparator;

import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.NullValueOption;

/**
 * Comparator for {@link KeyWrapper}.
 * This comparator uses the {@link ObjectInspector}s provided in the newObjectInspectors array to extract key values
 * from wrapped Object arrays by default.
 * {@link KeyWrapper} instances are cloned when {@link KeyWrapper#copyKey()} is called. In this case the wrapped
 * key values are deep copied and converted to standard objects using {@link ObjectInspectorUtils#copyToStandardObject}.
 * The comparator uses copyObjectInspectors when extracting key values from clones.
 */
public class KeyWrapperComparator implements Comparator<KeyWrapper> {

  private final ObjectInspector[] newObjectInspectors;
  private final ObjectInspector[] copyObjectInspectors;
  private final boolean[] columnSortOrderIsDesc;
  private final NullValueOption[] nullSortOrder;

  KeyWrapperComparator(ObjectInspector[] newObjectInspectors, ObjectInspector[] copyObjectInspectors,
                       String columnSortOrder, String nullSortOrder) {
    this.newObjectInspectors = newObjectInspectors;
    this.copyObjectInspectors = copyObjectInspectors;
    this.columnSortOrderIsDesc = new boolean[columnSortOrder.length()];
    this.nullSortOrder = new NullValueOption[nullSortOrder.length()];
    for (int i = 0; i < columnSortOrder.length(); ++i) {
      this.columnSortOrderIsDesc[i] = columnSortOrder.charAt(i) == '-';
      this.nullSortOrder[i] = NullOrdering.fromSign(nullSortOrder.charAt(i)).getNullValueOption();
    }
  }

  @Override
  public int compare(KeyWrapper key1, KeyWrapper key2) {
    return ObjectInspectorUtils.compare(
            key1.getKeyArray(),
            key1.isCopy() ? copyObjectInspectors : newObjectInspectors,
            key2.getKeyArray(),
            key2.isCopy() ? copyObjectInspectors : newObjectInspectors,
            columnSortOrderIsDesc,
            nullSortOrder);
  }
}
