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
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * LazyObject for storing a value of Double.
 * 
 */
public class LazyDouble extends
    LazyPrimitive<LazyDoubleObjectInspector, DoubleWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(LazyDouble.class);
  public LazyDouble(LazyDoubleObjectInspector oi) {
    super(oi);
    data = new DoubleWritable();
  }

  public LazyDouble(LazyDouble copy) {
    super(copy);
    data = new DoubleWritable(copy.data.get());
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
      data.set(Double.parseDouble(byteData));
      isNull = false;
    } catch (NumberFormatException e) {
      isNull = true;
      LOG.debug("Data not in the Double data type range so converted to null. Given data is :"
          + byteData, e);
    } catch (CharacterCodingException e) {
      isNull = true;
      LOG.debug("Data not in the Double data type range so converted to null.", e);
    }
  }

}
