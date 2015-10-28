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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
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

  public static final Logger LOG = LoggerFactory.getLogger(DelimitedJSONSerDe.class.getName());

  public DelimitedJSONSerDe() throws SerDeException {
  }

  /**
   * Not implemented.
   */
  @Override
  public Object doDeserialize(Writable field) throws SerDeException {
    LOG.error("DelimitedJSONSerDe cannot deserialize.");
    throw new SerDeException("DelimitedJSONSerDe cannot deserialize.");
  }

  @Override
  protected void serializeField(ByteStream.Output out, Object obj, ObjectInspector objInspector,
      LazySerDeParameters serdeParams) throws SerDeException {
    if (!objInspector.getCategory().equals(Category.PRIMITIVE) || (objInspector.getTypeName().equalsIgnoreCase(serdeConstants.BINARY_TYPE_NAME))) {
      //do this for all complex types and binary
      try {
        serialize(out, SerDeUtils.getJSONString(obj, objInspector, serdeParams.getNullSequence().toString()),
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, serdeParams.getSeparators(),
            1, serdeParams.getNullSequence(), serdeParams.isEscaped(), serdeParams.getEscapeChar(),
            serdeParams.getNeedsEscape());

      } catch (IOException e) {
        throw new SerDeException(e);
      }

    } else {
      //primitives except binary
      super.serializeField(out, obj, objInspector, serdeParams);
    }
  }
}
