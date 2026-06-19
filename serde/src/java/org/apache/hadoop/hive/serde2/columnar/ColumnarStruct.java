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

package org.apache.hadoop.hive.serde2.columnar;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * ColumnarStruct is different from LazyStruct in that ColumnarStruct's field
 * Object get parsed at its initialize time when call
 * {@link #init(BytesRefArrayWritable cols)}, while LazyStruct parse fields in a
 * lazy way.
 *
 */
public class ColumnarStruct extends ColumnarStructBase {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnarStruct.class);

  Text nullSequence;
  int lengthNullSequence;

  /**
   * Construct a ColumnarStruct object with the TypeInfo. It creates the first
   * level object at the first place
   *
   * @param oi
   *          the ObjectInspector representing the type of this LazyStruct.
   * @param notSkippedColumnIDs
   *          the column ids that should not be skipped
   */
  public ColumnarStruct(ObjectInspector oi, List<Integer> notSkippedColumnIDs, Text nullSequence) {
    super(oi, notSkippedColumnIDs);
    if (nullSequence != null) {
      this.nullSequence = nullSequence;
      this.lengthNullSequence = nullSequence.getLength();
    }
  }

  @Override
  protected int getLength(ObjectInspector objectInspector, ByteArrayRef cachedByteArrayRef,
      int start, int fieldLen) {
    if (fieldLen == lengthNullSequence) {
      byte[] data = cachedByteArrayRef.getData();
      if (LazyUtils.compare(data, start, fieldLen,
          nullSequence.getBytes(), 0, lengthNullSequence) == 0) {
        return -1;
      }
    }
    return fieldLen;
  }

  @Override
  protected LazyObjectBase createLazyObjectBase(ObjectInspector objectInspector) {
    return LazyFactory.createLazyObject(objectInspector);
  }
}
