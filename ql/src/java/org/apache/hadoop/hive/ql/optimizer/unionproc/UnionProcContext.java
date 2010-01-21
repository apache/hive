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

package org.apache.hadoop.hive.ql.optimizer.unionproc;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;

public class UnionProcContext implements NodeProcessorCtx {

  public static class UnionParseContext {
    transient private final boolean[] mapOnlySubq;
    transient private final boolean[] rootTask;
    transient private final boolean[] mapJoinSubq;

    transient private int numInputs;
    transient private boolean mapJoinQuery;

    public UnionParseContext(int numInputs) {
      this.numInputs = numInputs;
      mapOnlySubq = new boolean[numInputs];
      rootTask = new boolean[numInputs];
      mapJoinSubq = new boolean[numInputs];
    }

    public boolean getMapOnlySubq(int pos) {
      return mapOnlySubq[pos];
    }

    public void setMapOnlySubq(int pos, boolean mapOnlySubq) {
      this.mapOnlySubq[pos] = mapOnlySubq;
    }

    public boolean getMapJoinSubq(int pos) {
      return mapJoinSubq[pos];
    }

    public void setMapJoinSubq(int pos, boolean mapJoinSubq) {
      this.mapJoinSubq[pos] = mapJoinSubq;
      if (mapJoinSubq) {
        mapJoinQuery = true;
      }
    }

    public boolean getMapJoinQuery() {
      return mapJoinQuery;
    }

    public boolean getRootTask(int pos) {
      return rootTask[pos];
    }

    public void setRootTask(int pos, boolean rootTask) {
      this.rootTask[pos] = rootTask;
    }

    public int getNumInputs() {
      return numInputs;
    }

    public void setNumInputs(int numInputs) {
      this.numInputs = numInputs;
    }
  }

  // the subqueries are map-only jobs
  private boolean mapOnlySubq;

  /**
   * @return the mapOnlySubq
   */
  public boolean isMapOnlySubq() {
    return mapOnlySubq;
  }

  /**
   * @param mapOnlySubq
   *          the mapOnlySubq to set
   */
  public void setMapOnlySubq(boolean mapOnlySubq) {
    this.mapOnlySubq = mapOnlySubq;
  }

  private final Map<UnionOperator, UnionParseContext> uCtxMap;

  public UnionProcContext() {
    uCtxMap = new HashMap<UnionOperator, UnionParseContext>();
    mapOnlySubq = true;
  }

  public void setUnionParseContext(UnionOperator u, UnionParseContext uCtx) {
    uCtxMap.put(u, uCtx);
  }

  public UnionParseContext getUnionParseContext(UnionOperator u) {
    return uCtxMap.get(u);
  }
}
