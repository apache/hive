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
package org.apache.hadoop.hive.serde2.binarysortable;

import java.util.List;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class BinarySortableSerDeWithEndPrefix extends BinarySortableSerDe {
  public static void serializeStruct(Output byteStream, Object[] fieldData,
      List<ObjectInspector> fieldOis, boolean endPrefix) throws SerDeException {
    for (int i = 0; i < fieldData.length; i++) {
      serialize(byteStream, fieldData[i], fieldOis.get(i), false, ZERO, ONE);
    }
    if (endPrefix) {
      if (fieldData[fieldData.length-1]!=null) {
        byteStream.getData()[byteStream.getLength()-1]++;
      } else {
        byteStream.getData()[byteStream.getLength()-1]+=2;
      }
    }
  }
}