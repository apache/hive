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

package org.apache.hadoop.hive.ql.optimizer.signature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.stats.OperatorStats;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * This class makes it easier for jackson to comprehend the map type
 *
 * Instead of getting into convincing Jackson to store the map with serializers and typefactory tricks;
 * this class is a simple "repacker" to and from list.
 */
public final class RuntimeStatsMap {
  @JsonProperty
  private List<OpTreeSignature> sigs;
  @JsonProperty
  private List<OperatorStats> ss;

  RuntimeStatsMap() {
  }


  public RuntimeStatsMap(Map<OpTreeSignature, OperatorStats> input) {
    sigs = new ArrayList<>(input.size());
    ss = new ArrayList<>(input.size());
    for (Entry<OpTreeSignature, OperatorStats> ent : input.entrySet()) {
      sigs.add(ent.getKey());
      ss.add(ent.getValue());
    }
  }

  public Map<OpTreeSignature, OperatorStats> toMap() throws IOException {
    if (sigs.size() != ss.size()) {
      throw new IOException("constraint validation");
    }
    Map<OpTreeSignature, OperatorStats> ret = new HashMap<>();
    for (int i = 0; i < sigs.size(); i++) {
      ret.put(sigs.get(i), ss.get(i));
    }
    return ret;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sigs, ss);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != RuntimeStatsMap.class) {
      return false;
    }
    RuntimeStatsMap o = (RuntimeStatsMap) obj;
    return Objects.equal(sigs, o.sigs) &&
        Objects.equal(ss, o.ss);
  }
}
