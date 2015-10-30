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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyFloatObjectInspector;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

/**
 * LazyObject for storing a value of Double.
 * 
 */
public class LazyFloat extends
    LazyPrimitive<LazyFloatObjectInspector, FloatWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(LazyFloat.class);
  public LazyFloat(LazyFloatObjectInspector oi) {
    super(oi);
    data = new FloatWritable();
  }

  public LazyFloat(LazyFloat copy) {
    super(copy);
    data = new FloatWritable(copy.data.get());
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    String byteData = null;
    if (!LazyUtils.isNumberMaybe(bytes.getData(), start, length)) {
      isNull = true;
      return;
    }
    try {
      byteData = Text.decode(bytes.getData(), start, length);
      data.set(Float.parseFloat(byteData));
      isNull = false;
    } catch (NumberFormatException e) {
      isNull = true;
      LOG.debug("Data not in the Float data type range so converted to null. Given data is :"
          + byteData, e);
    } catch (CharacterCodingException e) {
      isNull = true;
      LOG.debug("Data not in the Float data type range so converted to null.", e);
    }
  }

}
