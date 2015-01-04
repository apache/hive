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
package org.apache.hadoop.hive.serde2.lazy;

import java.nio.charset.CharacterCodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * LazyPrimitive stores a primitive Object in a LazyObject.
 */
public abstract class LazyPrimitive<OI extends ObjectInspector, T extends Writable>
    extends LazyObject<OI> {

  private static final Log LOG = LogFactory.getLog(LazyPrimitive.class);
  protected LazyPrimitive(OI oi) {
    super(oi);
  }

  protected LazyPrimitive(LazyPrimitive<OI, T> copy) {
    super(copy.oi);
    isNull = copy.isNull;
  }

  protected T data;

  public T getWritableObject() {
    return isNull ? null : data;
  }

  @Override
  public String toString() {
    return isNull ? null : data.toString();
  }

  @Override
  public int hashCode() {
    return isNull ? 0 : data.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof LazyPrimitive<?, ?>)) {
      return false;
    }

    if (data == obj) {
      return true;
    }

    if (data == null || obj == null) {
      return false;
    }

    return data.equals(((LazyPrimitive<?, ?>) obj).getWritableObject());
  }

  public void logExceptionMessage(ByteArrayRef bytes, int start, int length, String dataType) {
    try {
      if(LOG.isDebugEnabled()) {
        String byteData = Text.decode(bytes.getData(), start, length);
        LOG.debug("Data not in the " + dataType
            + " data type range so converted to null. Given data is :" +
                    byteData, new Exception("For debugging purposes"));
      }
    } catch (CharacterCodingException e1) {
      LOG.debug("Data not in the " + dataType + " data type range so converted to null.", e1);
    }
  }

}
