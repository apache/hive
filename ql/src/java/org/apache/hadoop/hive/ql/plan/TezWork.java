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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.dag.api.EdgeProperty;

/**
 * TezWork. This class encapsulates all the work objects that can be executed
 * in a single tez job. Currently it's basically a tree with MapWork at the
 * leaves and and ReduceWork in all other nodes.
 *
 */
@SuppressWarnings("serial")
@Explain(displayName = "Tez")
public class TezWork extends AbstractOperatorDesc {

  private static transient final Log LOG = LogFactory.getLog(TezWork.class);

  private static int counter;
  private final String name;
  private final Set<BaseWork> roots = new HashSet<BaseWork>();
  private final Set<BaseWork> leaves = new HashSet<BaseWork>();
  private final Map<BaseWork, List<BaseWork>> workGraph = new HashMap<BaseWork, List<BaseWork>>();
  private final Map<BaseWork, List<BaseWork>> invertedWorkGraph = new HashMap<BaseWork, List<BaseWork>>();
  private final Map<Pair<BaseWork, BaseWork>, TezEdgeProperty> edgeProperties =
      new HashMap<Pair<BaseWork, BaseWork>, TezEdgeProperty>();

  public TezWork(String name) {
    this.name = name + ":" + (++counter);
  }

  @Explain(displayName = "DagName")
  public String getName() {
    return name;
  }

  /**
   * getWorkMap returns a map of "vertex name" to BaseWork
   */
  @Explain(displayName = "Vertices")
  public Map<String, BaseWork> getWorkMap() {
    Map<String, BaseWork> result = new LinkedHashMap<String, BaseWork>();
    for (BaseWork w: getAllWork()) {
      result.put(w.getName(), w);
    }
    return result;
  }

  /**
   * getAllWork returns a topologically sorted list of BaseWork
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
   * add all nodes in the collection without any connections
   */
  public void addAll(Collection<BaseWork> c) {
    for (BaseWork w: c) {
      this.add(w);
    }
  }

  /**
   * add all nodes in the collection without any connections
   */
  public void addAll(BaseWork[] bws) {
    for (BaseWork w: bws) {
      this.add(w);
    }
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
  }

  /**
   * getRoots returns all nodes that do not have a parent.
   */
  public Set<BaseWork> getRoots() {
    return new HashSet<BaseWork>(roots);
  }

  /**
   * getLeaves returns all nodes that do not have a child
   */
  public Set<BaseWork> getLeaves() {
    return new HashSet<BaseWork>(leaves);
  }

  /**
   * getParents returns all the nodes with edges leading into work
   */
  public List<BaseWork> getParents(BaseWork work) {
    assert invertedWorkGraph.containsKey(work)
      && invertedWorkGraph.get(work) != null;
    return new LinkedList<BaseWork>(invertedWorkGraph.get(work));
  }

  /**
   * getChildren returns all the nodes with edges leading out of work
   */
  public List<BaseWork> getChildren(BaseWork work) {
    assert workGraph.containsKey(work)
      && workGraph.get(work) != null;
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
      invertedWorkGraph.get(w).remove(work);
      if (invertedWorkGraph.get(w).size() == 0) {
        roots.add(w);
      }
    }

    for (BaseWork w: parents) {
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

  public EdgeType getEdgeType(BaseWork a, BaseWork b) {
    return edgeProperties.get(new ImmutablePair(a,b)).getEdgeType();
  }

  /**
   * returns the edge type connecting work a and b
   */
  public TezEdgeProperty getEdgeProperty(BaseWork a, BaseWork b) {
    return edgeProperties.get(new ImmutablePair(a,b));
  }

  /*
   * Dependency is a class used for explain
   */ 
  public class Dependency implements Serializable {
    public BaseWork w;
    public EdgeType type;

    @Explain(displayName = "Name")
    public String getName() {
      return w.getName();
    }
    
    @Explain(displayName = "Type")
    public String getType() {
      return type.toString();
    }
  }

  @Explain(displayName = "Edges")
  public Map<String, List<Dependency>> getDependencyMap() {
    Map<String, List<Dependency>> result = new LinkedHashMap<String, List<Dependency>>();
    for (Map.Entry<BaseWork, List<BaseWork>> entry: invertedWorkGraph.entrySet()) {
      List dependencies = new LinkedList<Dependency>();
      for (BaseWork d: entry.getValue()) {
        Dependency dependency = new Dependency();
        dependency.w = d;
        dependency.type = getEdgeType(d, entry.getKey());
        dependencies.add(dependency);
      }
      if (!dependencies.isEmpty()) {
        result.put(entry.getKey().getName(), dependencies);
      }
    }
    return result;
  }
  
  private static final String MR_JAR_PROPERTY = "tmpjars";
  /**
   * Calls configureJobConf on instances of work that are part of this TezWork.
   * Uses the passed job configuration to extract "tmpjars" added by these, so that Tez
   * could add them to the job proper Tez way. This is a very hacky way but currently
   * there's no good way to get these JARs - both storage handler interface, and HBase
   * code, would have to change to get the list directly (right now it adds to tmpjars).
   * This will happen in 0.14 hopefully.
   * @param jobConf Job configuration.
   * @return List of files added to tmpjars by storage handlers.
   */
  public String[] configureJobConfAndExtractJars(JobConf jobConf) {
    String[] oldTmpJars = jobConf.getStrings(MR_JAR_PROPERTY);
    jobConf.setStrings(MR_JAR_PROPERTY, new String[0]);
    for (BaseWork work : workGraph.keySet()) {
      work.configureJobConf(jobConf);
    }
    String[] newTmpJars = jobConf.getStrings(MR_JAR_PROPERTY);
    if (oldTmpJars != null && (oldTmpJars.length != 0)) {
      if (newTmpJars != null && (newTmpJars.length != 0)) {
        String[] combinedTmpJars = new String[newTmpJars.length + oldTmpJars.length];
        System.arraycopy(oldTmpJars, 0, combinedTmpJars, 0, oldTmpJars.length);
        System.arraycopy(newTmpJars, 0, combinedTmpJars, oldTmpJars.length, newTmpJars.length);
        jobConf.setStrings(MR_JAR_PROPERTY, combinedTmpJars);
      } else {
        jobConf.setStrings(MR_JAR_PROPERTY, oldTmpJars);
      }
    }
    return newTmpJars;
   }

  /**
   * connect adds an edge between a and b. Both nodes have
   * to be added prior to calling connect.
   * @param  
   */
  public void connect(BaseWork a, BaseWork b,
      TezEdgeProperty edgeProp) {
    workGraph.get(a).add(b);
    invertedWorkGraph.get(b).add(a);
    roots.remove(b);
    leaves.remove(a);
    ImmutablePair workPair = new ImmutablePair(a, b);
    edgeProperties.put(workPair, edgeProp);
  }
}
