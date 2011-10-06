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

package org.apache.hadoop.hive.serde2;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

/**
 * DelimitedJSONSerDe.
 *
 * This serde can only serialize, because it is just intended for use by the FetchTask class and the
 * TRANSFORM function.
 */
public class DelimitedJSONSerDe extends LazySimpleSerDe {

  public static final Log LOG = LogFactory.getLog(DelimitedJSONSerDe.class.getName());

  public DelimitedJSONSerDe() throws SerDeException {
  }

  /**
   * Not implemented.
   */
  @Override
  public Object deserialize(Writable field) throws SerDeException {
    LOG.error("DelimitedJSONSerDe cannot deserialize.");
    throw new SerDeException("DelimitedJSONSerDe cannot deserialize.");
  }

  @Override
  protected void serializeField(ByteStream.Output out, Object obj, ObjectInspector objInspector,
      SerDeParameters serdeParams) throws SerDeException {
    if (!objInspector.getCategory().equals(Category.PRIMITIVE) || (objInspector.getTypeName().equalsIgnoreCase(Constants.BINARY_TYPE_NAME))) {
      try {
        serialize(out, SerDeUtils.getJSONString(obj, objInspector),
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, serdeParams.getSeparators(),
            1, serdeParams.getNullSequence(), serdeParams.isEscaped(), serdeParams.getEscapeChar(),
            serdeParams.getNeedsEscape());

      } catch (IOException e) {
        throw new SerDeException(e);
      }

    } else {
      super.serializeField(out, obj, objInspector, serdeParams);
    }
  }
}
