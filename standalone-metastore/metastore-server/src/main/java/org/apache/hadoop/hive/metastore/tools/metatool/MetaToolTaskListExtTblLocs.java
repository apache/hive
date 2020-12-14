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
package org.apache.hadoop.hive.metastore.tools.metatool;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class MetaToolTaskListExtTblLocs extends MetaToolTask {

  private static final Logger LOG = LoggerFactory.getLogger(MetaToolTaskListExtTblLocs.class);
  private final HashMap<String, HashSet<String>> coverageList = new HashMap<>();
  private final HashMap<String, DataLocation> inputLocations = new HashMap<>();

  @Override
  void execute() {
    String[] loc = getCl().getListExtTblLocsParams();
    try{
      generateExternalTableInfo(loc[0], loc[1]);
    } catch (IOException | TException | JSONException e) {
      LOG.error("Listing external table locations failed: ", e);
    }
  }

  private void generateExternalTableInfo(String dbPattern, String outputDir) throws TException, IOException,
          JSONException {
    ObjectStore objectStore = getObjectStore();
    Configuration conf = msConf != null ? msConf : objectStore.getConf();
    String defaultCatalog = MetaStoreUtils.getDefaultCatalog(conf);
    List<String> databases = objectStore.getDatabases(defaultCatalog, dbPattern);
    System.out.println("Number of databases found for given pattern: " + databases.size());
    TreeSet<String> locations = new TreeSet<>();
    for (String db : databases) {
      List<String> tables = objectStore.getAllTables(defaultCatalog, db);
      for(String tblName : tables) {
        Table t = objectStore.getTable(defaultCatalog, db, tblName);
        if(TableType.EXTERNAL_TABLE.name().equalsIgnoreCase(t.getTableType())) {
          String tblLocation = t.getSd().getLocation();
          DataLocation dataLocation = new DataLocation(db, tblName, 0, 0,
                  null);
          inputLocations.put(tblLocation, dataLocation);
          if (!isCovered(locations, new Path(tblLocation))) {
            locations.add(tblLocation);
          }
          //retrieving partition locations outside table-location
          Map<String, String> partitionLocations = objectStore.getPartitionLocations(defaultCatalog, db, tblName,
                  tblLocation, -1);
          dataLocation.setTotalPartitions(partitionLocations.size());
          for (String partitionName : partitionLocations.keySet()) {
            String partLocation = partitionLocations.get(partitionName);
            //null value means partition is in table location, we do not add it to input in this case.
            if(partLocation == null) {
              dataLocation.incrementNumPartsInTblLoc();
            }
            else {
              partLocation = partLocation + Path.SEPARATOR + 
                      Warehouse.makePartName(Warehouse.makeSpecFromName(partitionName), false);
              inputLocations.put(partLocation, new DataLocation(db, tblName, 0,
                      0, partitionName));
              if(!isCovered(locations, new Path(partLocation))) {
                locations.add(partLocation);
              }
            }
          }
        }
      }
    }
    if(!locations.isEmpty()) {
      removeNestedStructure(locations);
      createOutputList(locations, outputDir, dbPattern);
    }
    else {
      System.out.println("No external tables found to process.");
    }
  }

  private boolean isPathWithinSubtree(Path path, Path subtree) {
    int subtreeDepth = subtree.depth();
    while(path != null){
      if (subtreeDepth > path.depth()) {
        return false;
      }
      if(subtree.equals(path)){
        return true;
      }
      path = path.getParent();
    }
    return false;
  }


  /*
   * Method to determine if an existing location covers the given location and record the coverage in output.
   */
  private boolean isCovered(TreeSet<String> locations, Path path) {
    Path originalPath = new Path(path.toString());
    while(path != null){
      if(locations.contains(path.toString())){
        addCoverage(path, originalPath, true);
        return true;
      }
      path = path.getParent();
    }
    return false;
  }

  /*
   * Method to cover a child node using a parent.
   * Removes the child and marks all nodes covered by the child as being covered by the parent.
   */
  private void addCoverage(Path parentPath, Path childPath, boolean addChild) {
    String childLoc = childPath.toString();
    String parentLoc = parentPath.toString();
    HashSet<String> pathsUnderChild = coverageList.get(childLoc);
    coverageList.remove(childLoc);
    if(coverageList.get(parentLoc) == null) {
      coverageList.put(parentLoc, new HashSet<>());
    }
    HashSet pathsUnderParent = coverageList.get(parentLoc);
    if(addChild) {
      pathsUnderParent.add(childPath.toString());
    }
    if(pathsUnderChild != null) {
      pathsUnderParent.addAll(pathsUnderChild);
      pathsUnderChild = null;
    }
  }

  /*
   * Transforms a collection so that no element is an ancestor of another.
   */
  private void removeNestedStructure(TreeSet<String> locations) {
    List<String> locationList = new ArrayList<>();
    locationList.addAll(locations);
    for(int i = 0; i < locationList.size(); i++) {
      String currLoc = locationList.get(i);
      Path currPath = new Path(currLoc);
      for(int j = i + 1; j < locationList.size(); j++) {
        String nextLoc = locationList.get(j);
        Path nextPath = new Path (nextLoc);
        if(isPathWithinSubtree(nextPath, currPath)) {
          addCoverage(currPath, nextPath, true);
          locations.remove(nextLoc);
          i = j;
        }
        else {
          i = j - 1;
          break;
        }
      }
    }
  }

  /*
   * Method to write the output to the given location.
   * We construct a tree out of external table - locations and use it to determine suitable directories covering all locations.
   */
  private void createOutputList(TreeSet<String> locations, String outputDir, String dbPattern) throws IOException, JSONException {
    ExternalTableGraphNode rootNode = constructTree(locations);
    //Traverse through the tree in breadth-first manner and decide which nodes to include.
    //For every node, either cover all leaves in its subtree using itself
    // or delegate this duty to its child nodes.
    Queue<ExternalTableGraphNode> queue = new LinkedList<>();
    queue.add(rootNode);
    while(!queue.isEmpty()){
      ExternalTableGraphNode current = queue.remove();
      if(current.isLeaf()) {
        // in this case, the leaf needs to be added to the solution, i.e. marked as being covered.
        // This was done during graph construction, so we continue.
        continue;
      }
      int nonTrivialCoverage = 0;
      List<ExternalTableGraphNode> childNodes = current.getChildNodes();
      for(ExternalTableGraphNode child : childNodes) {
        if (child.getNumLeavesCovered() > 1) {
          nonTrivialCoverage += child.getNumLeavesCovered();
        }
      }
      int numLeavesCovered = current.getNumLeavesCovered();
      if((nonTrivialCoverage >= (numLeavesCovered + 1) / 2) || numLeavesCovered == 1) {
        queue.addAll(childNodes);
      } else {
        addToSolution(current);
      }
    }
    String outFileName = "externalTableLocations_" + dbPattern + "_" + System.currentTimeMillis() + ".txt";
    System.out.println("Writing output to " + outFileName);
    FileWriter fw = new FileWriter(outputDir + "/" + outFileName);
    PrintWriter pw = new PrintWriter(fw);
    JSONObject jsonObject = new JSONObject();
    for(String outputLocation : coverageList.keySet()) {
      HashSet<String> coveredLocations = coverageList.get(outputLocation);
      JSONArray outputEntities = listOutputEntities(coveredLocations);
      jsonObject.put(outputLocation, outputEntities);
    }
    pw.println(jsonObject.toString(4).replace("\\",""));
    pw.close();
  }

  /*
   * Returns a comma separated list of entities(tables or partition names) covered by to a location.
   * Table-name followed by "*" indicates that all partitions are inside table location.
   * Otherwise, we record the number of partitions covered by table location.
   */
  private JSONArray listOutputEntities(HashSet<String> locations) {
    List<String> listEntities = new ArrayList<>();
    for(String loc : locations) {
      DataLocation data = inputLocations.get(loc);
      String out = data.getDbName() + "." + data.getTblName();
      String partName = data .getPartName();
      if (partName == null) {
        int numPartInTblLoc = data.getNumPartitionsInTblLoc();
        int totPartitions = data.getTotalPartitions();
        if (totPartitions > 0 && numPartInTblLoc == totPartitions) {
          out = out + ".*";
        }
        else if (totPartitions > 0) {
          out = out + " p(" + numPartInTblLoc + "/" + totPartitions + ")";
        }
      }
      else {
        out = out + "." + partName;
      }
      listEntities.add(out);
    }
    Collections.sort(listEntities);
    return new JSONArray(listEntities);
  }

  private ExternalTableGraphNode constructTree(TreeSet<String> locations) {
    ExternalTableGraphNode rootNode = null;
    HashMap<String, ExternalTableGraphNode> locationGraph = new HashMap<>();
    // Every location is represented by a leaf in the tree.
    // We traverse through the input locations and construct the tree.
    for (String leaf : locations) {
      ExternalTableGraphNode currNode = new ExternalTableGraphNode(leaf, new ArrayList<>(), true);
      locationGraph.put(leaf, currNode);
      if (coverageList.get(leaf) == null) {
        coverageList.put(leaf, new HashSet<>());
      }
      //mark the leaf as being covered as itself
      HashSet currCoverage = coverageList.get(leaf);
      currCoverage.add(leaf);
      currNode.setNumLeavesCovered(currCoverage.size());
      Path parent = new Path(leaf).getParent();
      ExternalTableGraphNode parNode;
      //traverse upward to the root in order to construct the graph
      while (parent != null) {
        String parentLoc = parent.toString();
        if (!locationGraph.containsKey(parentLoc)) {
          //if parent doesn't exist in graph then create it
          parNode = new ExternalTableGraphNode(parentLoc, new ArrayList<>(), false);
          locationGraph.put(parentLoc, parNode);
        }
        else {
          parNode = locationGraph.get(parentLoc);
        }
        if(currNode.getParent() == null) {
          parNode.addChild(currNode);
          currNode.setParent(parNode);
        }
        else {
          break;
        }
        currNode = parNode;
        parent = parent.getParent();
      }
      if (parent == null && rootNode == null) {
        rootNode = currNode;
        rootNode.setParent(rootNode);
      }
    }
    rootNode.updateNumLeavesCovered();
    return rootNode;
  }

  private void addToSolution(ExternalTableGraphNode node) {
    //since this node is in the solution, all its children should be covered using this node.
    if(!node.isLeaf()) {
      addCoverageRecursive(node);
    }
  }

  private void addCoverageRecursive(ExternalTableGraphNode node) {
    for(ExternalTableGraphNode child : node.getChildNodes()) {
      if(child.isLeaf()) {
        addCoverage(new Path(node.getLocation()), new Path(child.getLocation()), true);
      }
      else {
        addCoverageRecursive(child);
        addCoverage(new Path(node.getLocation()), new Path(child.getLocation()), false);
      }
    }
  }
  
  @VisibleForTesting
  static Configuration msConf = null;

  /*
   * Class denoting every external table data location.
   * Each location can be either a table location(in this case, partition-name is not set) or
   * a partition location which is outside table location.
   * If the location is a table location, we store additional data like how many partitions are there in the table 
   * and how many of them are there in the table loc itself.
   */
  private class DataLocation {
    private String dbName;
    private String tblName;
    private int numPartitionsInTblLoc;
    private String partName;
    private int totalPartitions;

    private DataLocation (String dbName, String tblName, int totalPartitions, int numPartitionsInTblLoc,
                          String partName) {
      this.dbName = dbName;
      this.tblName = tblName;
      this.totalPartitions = totalPartitions;
      this.numPartitionsInTblLoc = numPartitionsInTblLoc;
      this.partName = partName;
    }

    private void incrementNumPartsInTblLoc() {
      this.numPartitionsInTblLoc++;
    }
    
    private String getPartName() {
      return this.partName;
    }

    private String getDbName() {
      return this.dbName;
    }
    
    private String getTblName() {
      return this.tblName;
    }
    
    private int getNumPartitionsInTblLoc() {
      return this.numPartitionsInTblLoc;
    }

    private int getTotalPartitions() {
      return this.totalPartitions;
    }
    
    private void setTotalPartitions(int totalPartitions) {
      this.totalPartitions = totalPartitions;
    }
  }

  private class ExternalTableGraphNode {
    private String location;
    private List<ExternalTableGraphNode> childNodes;
    private ExternalTableGraphNode parent;
    private boolean isLeaf;
    private int numLeavesCovered;

    private ExternalTableGraphNode(String location, List<ExternalTableGraphNode> childNodes, boolean isLeaf) {
      this.location = location;
      this.childNodes = childNodes;
      this.isLeaf = isLeaf;
      this.parent = null;
    }

    private void addChild(ExternalTableGraphNode child) {
      this.childNodes.add(child);
    }
    
    private List<ExternalTableGraphNode> getChildNodes() {
      return this.childNodes;
    }

    private boolean isLeaf() {
      return this.isLeaf;
    }

    private void setNumLeavesCovered(int numLeavesCovered) {
      this.numLeavesCovered = numLeavesCovered;
    }

    private int getNumLeavesCovered() {
      return this.numLeavesCovered;
    }

    private String getLocation() {
      return this.location;
    }

    private void setParent(ExternalTableGraphNode node) {
      this.parent = node;
    }

    private ExternalTableGraphNode getParent() {
      return this.parent;
    }

    private void updateNumLeavesCovered() {
      if(this.isLeaf) {
        return;
      }
      this.numLeavesCovered = 0;
      for(ExternalTableGraphNode currChild : childNodes) {
        currChild.updateNumLeavesCovered();
        this.numLeavesCovered += currChild.getNumLeavesCovered();
      }
    }
  }
}
