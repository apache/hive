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

package org.apache.hadoop.hive.ql.anon.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeDataEntry;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_KEY_TYPE;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.INDEX_VALUE_TYPES;

public class SimpleConverter implements Converter {

  private final List<Byte> keyFieldTypes = new ArrayList<>();
  private final List<Byte> valueFieldTypes = new ArrayList<>();
  private final Configuration conf;

  public SimpleConverter(final Configuration conf) {
    this.conf = conf;
  }

  @Override
  public RawDataEntry convert(BtreeDataEntry dataEntry) {
    if (keyFieldTypes.isEmpty()) {
      final String keyType = conf.get(INDEX_KEY_TYPE);
      final String valueTypes = conf.get(INDEX_VALUE_TYPES);

      for (char c : keyType.toCharArray()) {
        keyFieldTypes.add((byte) c);
      }
      for (char c : valueTypes.toCharArray()) {
        valueFieldTypes.add((byte) c);
      }
    }
    return Converters.convert(dataEntry);
  }

  @Override
  public List<Byte> keyFieldTypes() {
    return keyFieldTypes;
  }

  @Override
  public List<Byte> valueFieldTypes() {
    return valueFieldTypes;
  }
}
