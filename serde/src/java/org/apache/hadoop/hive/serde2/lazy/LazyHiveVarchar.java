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
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Text;

/**
 * LazyObject for storing a value of HiveVarchar.
 *
 */
public class LazyHiveVarchar extends
    LazyPrimitive<LazyHiveVarcharObjectInspector, HiveVarcharWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(LazyHiveVarchar.class);

  protected int maxLength = -1;

  public LazyHiveVarchar(LazyHiveVarcharObjectInspector oi) {
    super(oi);
    maxLength = ((VarcharTypeInfo)oi.getTypeInfo()).getLength();
    data = new HiveVarcharWritable();
  }

  public LazyHiveVarchar(LazyHiveVarchar copy) {
    super(copy);
    this.maxLength = copy.maxLength;
    data = new HiveVarcharWritable(copy.data);
  }

  public void setValue(LazyHiveVarchar copy) {
    data.set(copy.data, maxLength);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    if (oi.isEscaped()) {
      Text textData =  data.getTextValue();
      // This is doing a lot of copying here, this could be improved by enforcing length
      // at the same time as escaping rather than as separate steps.
      LazyUtils.copyAndEscapeStringDataToText(bytes.getData(), start, length,
          oi.getEscapeChar(),textData);
      data.set(textData.toString(), maxLength);
      isNull = false;
    } else {
      try {
        String byteData = null;
        byteData = Text.decode(bytes.getData(), start, length);
        data.set(byteData, maxLength);
        isNull = false;
      } catch (CharacterCodingException e) {
        isNull = true;
        LOG.debug("Data not in the HiveVarchar data type range so converted to null.", e);
      }
    }
  }

}
