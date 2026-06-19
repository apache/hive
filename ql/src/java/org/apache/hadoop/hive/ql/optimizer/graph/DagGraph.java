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

package org.apache.hadoop.hive.ql.optimizer.graph;

import java.util.HashSet;
import java.util.Set;


/**
 * A directed graph extended with support to check dag property.
 */
class DagGraph<V, E> extends DiGraph<V, E> {

  static class DagNode<V, E> extends Node<V, E> {
    int dagIdx = 0;
    public DagNode(V v) {
      super(v);
    }

    @Override
    public void addEdge(Edge<V, E> edge) {
      if (edge.s == this) {
        DagGraph.DagNode<V, E> t = (DagGraph.DagNode<V, E>) edge.t;
        ensureDagIdxAtLeast(t.dagIdx + 1);
        if (t.dagIdx > dagIdx) {
          throw new IllegalArgumentException("adding this edge would violate dag properties");
        }
      }
      super.addEdge(edge);
    }

    void ensureDagIdxAtLeast(int min) {
      if (dagIdx >= min) {
        return;
      }
      dagIdx = min;
      for (Edge<V, E> e : edges) {
        if(e.t == this) {
          DagGraph.DagNode<V, E> s = (DagGraph.DagNode<V, E>) e.s;
          s.ensureDagIdxAtLeast(min + 1);
        }
      }
    }
  }

  @Override
  protected Node<V, E> newNode(V s) {
    return new DagGraph.DagNode<V, E>(s);
  }

  public int getDepth(V o1) {
    Set<Node<V, E>> active = new HashSet<>();
    active.add(nodeOf(o1));
    int depth = 0;
    while(!active.isEmpty()) {
      Set<Node<V, E>> newActive = new HashSet<>();
      for (Node<V, E> node : active) {
        Set<V> p = successors(node.v);
        for (V n : p) {
          newActive.add(nodeOf(n));
        }
      }
      depth++;
      active = newActive;
    }
    return depth;
  }

}