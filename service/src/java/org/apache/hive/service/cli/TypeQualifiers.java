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

package org.apache.hive.service.cli;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde2.typeinfo.BaseTypeParams;
import org.apache.hive.service.cli.thrift.TCLIServiceConstants;
import org.apache.hive.service.cli.thrift.TTypeQualifierValue;
import org.apache.hive.service.cli.thrift.TTypeQualifiers;

/**
 * This class holds type qualifier information for a primitive type,
 * such as char/varchar length or decimal precision/scale.
 */
public class TypeQualifiers {
  private Integer characterMaximumLength;

  public TypeQualifiers() {}

  public Integer getCharacterMaximumLength() {
    return characterMaximumLength;
  }
  public void setCharacterMaximumLength(int characterMaximumLength) {
    this.characterMaximumLength = characterMaximumLength;
  }

  public TTypeQualifiers toTTypeQualifiers() {
    TTypeQualifiers ret = null;

    Map<String, TTypeQualifierValue> qMap = new HashMap<String, TTypeQualifierValue>();
    if (getCharacterMaximumLength() != null) {
      TTypeQualifierValue val = new TTypeQualifierValue();
      val.setI32Value(getCharacterMaximumLength().intValue());
      qMap.put(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH, val);
    }

    if (qMap.size() > 0) {
      ret = new TTypeQualifiers(qMap);
    }

    return ret;
  }

  public static TypeQualifiers fromTTypeQualifiers(TTypeQualifiers ttq) {
    TypeQualifiers ret = null;
    if (ttq != null) {
      ret = new TypeQualifiers();
      Map<String, TTypeQualifierValue> tqMap = ttq.getQualifiers();

      if (tqMap.containsKey(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH)) {
        ret.setCharacterMaximumLength(
            tqMap.get(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH).getI32Value());
      }
    }
    return ret;
  }

  public static TypeQualifiers fromBaseTypeParams(BaseTypeParams typeParams) {
    TypeQualifiers ret = null;
    if (typeParams != null) {
      ret = new TypeQualifiers();
      if (typeParams.hasCharacterMaximumLength()) {
        ret.setCharacterMaximumLength(typeParams.getCharacterMaximumLength());
      }
    }
    return ret;
  }
}
