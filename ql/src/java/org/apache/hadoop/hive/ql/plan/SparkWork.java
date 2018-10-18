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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import com.google.common.base.Preconditions;

/**
 * This class encapsulates all the work objects that can be executed
 * in a single Spark job. Currently it's basically a tree with MapWork at the
 * roots and and ReduceWork at all other nodes.
 */
@SuppressWarnings("serial")
@Explain(displayName = "Spark", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      vectorization = Vectorization.SUMMARY_PATH)
public class SparkWork extends AbstractOperatorDesc {

  private static final AtomicInteger counter = new AtomicInteger(1);
  private final String dagName;
  private final String queryId;

  private final Set<BaseWork> roots = new LinkedHashSet<BaseWork>();
  private final Set<BaseWork> leaves = new LinkedHashSet<>();

  protected final Map<BaseWork, List<BaseWork>> workGraph =
      new LinkedHashMap<BaseWork, List<BaseWork>>();
  protected final Map<BaseWork, List<BaseWork>> invertedWorkGraph =
      new LinkedHashMap<BaseWork, List<BaseWork>>();
  protected final Map<Pair<BaseWork, BaseWork>, SparkEdgeProperty> edgeProperties =
      new HashMap<Pair<BaseWork, BaseWork>, SparkEdgeProperty>();

  private Map<String, List<String>> requiredCounterPrefix;

  private Map<BaseWork, BaseWork> cloneToWork;

  public SparkWork(String queryId) {
    this.queryId = queryId;
    this.dagName = queryId + ":" + counter.getAndIncrement();
    cloneToWork = new HashMap<BaseWork, BaseWork>();
  }

  @Explain(displayName = "DagName")
  public String getName() {
    return this.dagName;
  }

  public String getQueryId() {
    return this.queryId;
  }

  /**
   * @return a map of "vertex name" to BaseWork
   */
  @Explain(displayName = "Vertices", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED },
      vectorization = Vectorization.SUMMARY_PATH)
  public Map<String, BaseWork> getWorkMap() {
    Map<String, BaseWork> result = new LinkedHashMap<String, BaseWork>();
    for (BaseWork w: getAllWork()) {
      result.put(w.getName(), w);
    }
    return result;
  }

  /**
   * @return a topologically sorted list of BaseWork
   */
  public List<BaseWork> getAllWork() {

    List<BaseWork> result = new LinkedList<BaseWork>();
    Set<BaseWork> seen = new HashSet<BaseWork>();

    for (BaseWork leaf: leaves) {
      // make sure all leaves are visited at least once
      visit(leaf, seen, result);
    }

    return result;
  }

  public Collection<BaseWork> getAllWorkUnsorted() {
    return workGraph.keySet();
  }

  private void visit(BaseWork child, Set<BaseWork> seen, List<BaseWork> result) {
    if (seen.contains(child)) {
      // don't visit multiple times
      return;
    }

    seen.add(child);

    for (BaseWork parent: getParents(child)) {
      if (!seen.contains(parent)) {
        visit(parent, seen, result);
      }
    }

    result.add(child);
  }

  /**
   * Add all nodes in the collection without any connections.
   */
  public void addAll(Collection<BaseWork> c) {
    for (BaseWork w: c) {
      this.add(w);
    }
  }

  /**
   * Add all nodes in the collection without any connections.
   */
  public void addAll(BaseWork[] bws) {
    for (BaseWork w: bws) {
      this.add(w);
    }
  }

  /**
   * Whether the specified BaseWork is a vertex in this graph
   * @param w the BaseWork to check
   * @return whether specified BaseWork is in this graph
   */
  public boolean contains(BaseWork w) {
    return workGraph.containsKey(w);
  }

  /**
   * add creates a new node in the graph without any connections
   */
  public void add(BaseWork w) {
    if (workGraph.containsKey(w)) {
      return;
    }
    workGraph.put(w, new LinkedList<BaseWork>());
    invertedWorkGraph.put(w, new LinkedList<BaseWork>());
    roots.add(w);
    leaves.add(w);
  }

  /**
   * disconnect removes an edge between a and b. Both a and
   * b have to be in the graph. If there is no matching edge
   * no change happens.
   */
  public void disconnect(BaseWork a, BaseWork b) {
    workGraph.get(a).remove(b);
    invertedWorkGraph.get(b).remove(a);
    if (getParents(b).isEmpty()) {
      roots.add(b);
    }
    if (getChildren(a).isEmpty()) {
      leaves.add(a);
    }
    edgeProperties.remove(new ImmutablePair<BaseWork, BaseWork>(a, b));
  }

  /**
   * getRoots returns all nodes that do not have a parent.
   */
  public Set<BaseWork> getRoots() {
    return new LinkedHashSet<BaseWork>(roots);
  }

  /**
   * getLeaves returns all nodes that do not have a child
   */
  public Set<BaseWork> getLeaves() {
    return new LinkedHashSet<BaseWork>(leaves);
  }

  public void setRequiredCounterPrefix(Map<String, List<String>> requiredCounterPrefix) {
    this.requiredCounterPrefix = requiredCounterPrefix;
  }

  public Map<String, List<String>> getRequiredCounterPrefix() {
    return requiredCounterPrefix;
  }

  /**
   * getParents returns all the nodes with edges leading into work
   */
  public List<BaseWork> getParents(BaseWork work) {
    Preconditions.checkArgument(invertedWorkGraph.containsKey(work),
        "AssertionError: expected invertedWorkGraph.containsKey(work) to be true");
    Preconditions.checkArgument(invertedWorkGraph.get(work) != null,
        "AssertionError: expected invertedWorkGraph.get(work) to be not null");
    return new LinkedList<BaseWork>(invertedWorkGraph.get(work));
  }

  /**
   * getChildren returns all the nodes with edges leading out of work
   */
  public List<BaseWork> getChildren(BaseWork work) {
    Preconditions.checkArgument(workGraph.containsKey(work),
        "AssertionError: expected workGraph.containsKey(work) to be true");
    Preconditions.checkArgument(workGraph.get(work) != null,
        "AssertionError: expected workGraph.get(work) to be not null");
    return new LinkedList<BaseWork>(workGraph.get(work));
  }

  /**
   * remove removes a node from the graph and removes all edges with
   * work as start or end point. No change to the graph if the node
   * doesn't exist.
   */
  public void remove(BaseWork work) {
    if (!workGraph.containsKey(work)) {
      return;
    }

    List<BaseWork> children = getChildren(work);
    List<BaseWork> parents = getParents(work);

    for (BaseWork w: children) {
      edgeProperties.remove(new ImmutablePair<BaseWork, BaseWork>(work, w));
      invertedWorkGraph.get(w).remove(work);
      if (invertedWorkGraph.get(w).size() == 0) {
        roots.add(w);
      }
    }

    for (BaseWork w: parents) {
      edgeProperties.remove(new ImmutablePair<BaseWork, BaseWork>(w, work));
      workGraph.get(w).remove(work);
      if (workGraph.get(w).size() == 0) {
        leaves.add(w);
      }
    }

    roots.remove(work);
    leaves.remove(work);

    workGraph.remove(work);
    invertedWorkGraph.remove(work);
  }

  /**
   * returns the edge type connecting work a and b
   */
  public SparkEdgeProperty getEdgeProperty(BaseWork a, BaseWork b) {
    return edgeProperties.get(new ImmutablePair<BaseWork, BaseWork>(a, b));
  }

  /**
   * connect adds an edge between a and b. Both nodes have
   * to be added prior to calling connect.
   * @param
   */
  public void connect(BaseWork a, BaseWork b, SparkEdgeProperty edgeProp) {
    workGraph.get(a).add(b);
    invertedWorkGraph.get(b).add(a);
    roots.remove(b);
    leaves.remove(a);
    ImmutablePair<BaseWork, BaseWork> workPair = new ImmutablePair<BaseWork, BaseWork>(a, b);
    edgeProperties.put(workPair, edgeProp);
  }

  /*
   * Dependency is a class used for explain
   */
  public class Dependency implements Serializable, Comparable<Dependency> {
    public BaseWork w;
    public SparkEdgeProperty prop;

    @Explain(displayName = "Name")
    public String getName() {
      return w.getName();
    }

    @Explain(displayName = "Shuffle Type")
    public String getShuffleType() {
      return prop.getShuffleType();
    }

    @Explain(displayName = "Number of Partitions")
    public String getNumPartitions() {
      return Integer.toString(prop.getNumPartitions());
    }

    @Override
    public int compareTo(Dependency o) {
      int compare = getName().compareTo(o.getName());
      if (compare == 0) {
        compare = getShuffleType().compareTo(o.getShuffleType());
      }
      return compare;
    }
  }

  /**
   * Task name is usually sorted by natural order, which is the same
   * as the topological order in most cases. However, with Spark, some
   * tasks may be converted, so have new names. The natural order may
   * be different from the topological order. This class is to make
   * sure all tasks to be sorted by topological order deterministically.
   */
  private static class ComparableName implements Comparable<ComparableName> {
    private final Map<String, String> dependencies;
    private final String name;

    ComparableName(Map<String, String> dependencies, String name) {
      this.dependencies = dependencies;
      this.name = name;
    }

    /**
     * Check if task n1 depends on task n2
     */
    boolean dependsOn(String n1, String n2) {
      for (String p = dependencies.get(n1); p != null; p = dependencies.get(p)) {
        if (p.equals(n2)) {
          return true;
        }
      }
      return false;
    }

    /**
     * Get the number of parents of task n
     */
    int getDepth(String n) {
      int depth = 0;
      for (String p = dependencies.get(n); p != null; p = dependencies.get(p)) {
        depth++;
      }
      return depth;
    }

    @Override
    public int compareTo(ComparableName o) {
      if (dependsOn(name, o.name)) {
        // this depends on o
        return 1;
      }
      if (dependsOn(o.name, name)) {
        // o depends on this
        return -1;
      }
      // No dependency, check depth
      int d1 = getDepth(name);
      int d2 = getDepth(o.name);
      if (d1 == d2) {
        // Same depth, using natural order
        return name.compareTo(o.name);
      }
      // Deep one is bigger, i.e. less to the top
      return d1 > d2 ? 1 : -1;
    }

    @Override
    public String toString() {
      return name;
    }
   }

  @Explain(displayName = "Edges", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED },
      vectorization = Vectorization.SUMMARY_PATH)
  public Map<ComparableName, List<Dependency>> getDependencyMap() {
    Map<String, String> allDependencies = new HashMap<String, String>();
    Map<ComparableName, List<Dependency>> result =
      new LinkedHashMap<ComparableName, List<Dependency>>();
    for (BaseWork baseWork : getAllWork()) {
      if (invertedWorkGraph.get(baseWork) != null && invertedWorkGraph.get(baseWork).size() > 0) {
        List<Dependency> dependencies = new LinkedList<Dependency>();
        for (BaseWork d : invertedWorkGraph.get(baseWork)) {
          allDependencies.put(baseWork.getName(), d.getName());
          Dependency dependency = new Dependency();
          dependency.w = d;
          dependency.prop = getEdgeProperty(d, baseWork);
          dependencies.add(dependency);
        }
        if (!dependencies.isEmpty()) {
          Collections.sort(dependencies);
          result.put(new ComparableName(allDependencies,
            baseWork.getName()), dependencies);
        }
      }
    }
    return result;
  }

  /**
   * @return all reduce works of this spark work, in sorted order.
   */
  public List<ReduceWork> getAllReduceWork() {
    List<ReduceWork> result = new ArrayList<ReduceWork>();
    for (BaseWork work : getAllWork()) {
      if (work instanceof ReduceWork) {
        result.add((ReduceWork) work);
      }
    }
    return result;
  }

  public Map<BaseWork, BaseWork> getCloneToWork() {
    return cloneToWork;
  }

  public void setCloneToWork(Map<BaseWork, BaseWork> cloneToWork) {
    this.cloneToWork = cloneToWork;
  }
}
