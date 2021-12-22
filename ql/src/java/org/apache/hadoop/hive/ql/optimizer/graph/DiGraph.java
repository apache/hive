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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;


import java.util.Optional;

public class DiGraph<V, E> {
  protected static class Edge<V, E> {
    public final DiGraph.Node<V, E> s;
    public final DiGraph.Node<V, E> t;
    public final E e;

    public Edge(DiGraph.Node<V, E> s, DiGraph.Node<V, E> t, E e) {
      this.s = s;
      this.t = t;
      this.e = e;
    }
  }

  protected static class Node<V, E> {
    public final Set<DiGraph.Edge<V, E>> edges;
    public final V v;

    public Node(V v) {
      edges = new LinkedHashSet<>();
      this.v = v;
    }

    public void addEdge(DiGraph.Edge<V, E> edge) {
      if (edge.s == this) {
        edge.t.addEdge(edge);
      }
      edges.add(edge);
    }

    public E removeEdge(V s, V t) {
      Iterator<DiGraph.Edge<V, E>> it = edges.iterator();
      while (it.hasNext()) {
        DiGraph.Edge<V, E> edge = it.next();
        if (edge.s.v.equals(s) && edge.t.v.equals(t)) {
          it.remove();
          return edge.e;
        }
      }
      return null;
    }

    public int degree() {
      return edges.size();
    }

    public int inDegree() {
      Iterator<DiGraph.Edge<V, E>> it = edges.iterator();
      int cnt = 0;
      while (it.hasNext()) {
        DiGraph.Edge<V, E> edge = it.next();
        if (edge.t == this) {
          cnt++;
        }
      }
      return cnt;
    }



  }

  private final Map<V, DiGraph.Node<V, E>> nodes;

  public DiGraph() {
    nodes = new LinkedHashMap<>();
  }

  public void putEdgeValue(V s, V t, E e) {
    DiGraph.Node<V, E> nodeS = nodeOf(s);
    DiGraph.Node<V, E> nodeT = nodeOf(t);
    DiGraph.Edge<V, E> edge = new DiGraph.Edge<>(nodeS, nodeT, e);
    nodeS.addEdge(edge);
    //      nodeT.addEdge(edge);
  }

  public DiGraph.Node<V, E> nodeOf(V s) {
    DiGraph.Node<V, E> node = nodes.get(s);
    if (node == null) {
      nodes.put(s, node = newNode(s));
    }
    return node;
  }

  protected DiGraph.Node<V, E> newNode(V s) {
    return new DiGraph.Node<>(s);
  }

  public Set<V> nodes() {
    return nodes.keySet();
  }

  public Set<V> predecessors(V n) {
    Set<V> ret = new LinkedHashSet<>();
    DiGraph.Node<V, E> node = nodes.get(n);
    if (node == null) {
      return ret;
    }

    for (DiGraph.Edge<V, E> edge : node.edges) {
      if (edge.t.v.equals(n)) {
        ret.add(edge.s.v);
      }
    }
    return ret;
  }

  public Set<V> successors(V n) {
    Set<V> ret = new LinkedHashSet<>();
    DiGraph.Node<V, E> node = nodes.get(n);
    if (node == null) {
      return ret;
    }

    for (DiGraph.Edge<V, E> edge : node.edges) {
      if (edge.s.v.equals(n)) {
        ret.add(edge.t.v);
      }
    }
    return ret;
  }

  public Optional<E> getEdge(V s, V t) {
    DiGraph.Node<V, E> node = nodes.get(s);
    if (node == null) {
      return Optional.empty();
    }

    for (DiGraph.Edge<V, E> edge : node.edges) {
      if (edge.s.v.equals(s) && edge.t.v.equals(t)) {
        return Optional.of(edge.e);
      }
    }
    return Optional.empty();
  }

  public int degree(V n) {
    DiGraph.Node<V, E> node = nodes.get(n);
    return node.degree();
  }

  public int inDegree(V n) {
    DiGraph.Node<V, E> node = nodes.get(n);
    return node.inDegree();
  }

  public E removeEdge(V s, V t) {
    nodeOf(s).removeEdge(s, t);
    return nodeOf(t).removeEdge(s, t);
  }

  public void impode(V n, E edge) {
    Set<V> preds = predecessors(n);
    Set<V> succ = successors(n);

    for (V u : preds) {
      for (V v : succ) {
        putEdgeValue(u, v, edge);
      }
    }
    delete(n);


  }

  public void delete(V n) {
    DiGraph.Node<V, E> node = nodeOf(n);
    Set<DiGraph.Edge<V, E>> edges = new HashSet<>(node.edges);
    for (DiGraph.Edge<V, E> edge : edges) {
      removeEdge(edge.s.v, edge.t.v);
    }
    nodes.remove(n);
  }

}