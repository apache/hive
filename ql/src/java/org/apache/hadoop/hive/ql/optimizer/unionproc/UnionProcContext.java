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

package org.apache.hadoop.hive.ql.optimizer.unionproc;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;

/**
 * UnionProcContext.
 *
 */
public class UnionProcContext implements NodeProcessorCtx {

  /**
   * UnionParseContext.
   *
   */
  public static class UnionParseContext {
    private final transient boolean[] mapOnlySubq;
    private final transient boolean[] mapOnlySubqSet;
    private final transient boolean[] rootTask;

    private final transient int numInputs;

    public UnionParseContext(int numInputs) {
      this.numInputs = numInputs;
      mapOnlySubq = new boolean[numInputs];
      rootTask = new boolean[numInputs];
      mapOnlySubqSet = new boolean[numInputs];
    }

    public boolean getMapOnlySubq(int pos) {
      return mapOnlySubq[pos];
    }

    public void setMapOnlySubq(int pos, boolean mapOnlySubq) {
      this.mapOnlySubq[pos] = mapOnlySubq;
      this.mapOnlySubqSet[pos] = true;
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

    public boolean allMapOnlySubQ() {
      return isAllTrue(mapOnlySubq);
    }

    public boolean allMapOnlySubQSet() {
      return isAllTrue(mapOnlySubqSet);
    }

    public boolean allRootTasks() {
      return isAllTrue(rootTask);
    }

    public boolean isAllTrue(boolean[] array) {
      for (boolean value : array) {
        if (!value) {
          return false;
        }
      }
      return true;
    }
  }

  // the subqueries are map-only jobs
  private boolean mapOnlySubq;

  // ParseContext
  private ParseContext parseContext;

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

  public ParseContext getParseContext() {
    return parseContext;
  }

  public void setParseContext(ParseContext parseContext) {
    this.parseContext = parseContext;
  }
}
