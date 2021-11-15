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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

public class MetaToolTaskListExtTblLocs extends MetaToolTask {
  private static final Logger LOG = LoggerFactory.getLogger(MetaToolTaskListExtTblLocs.class);
  private static Configuration conf;
  private final Map<String, HashSet<String>> coverageList = new HashMap<>(); //maps each output-location to the set of input-locations covered by it
  private final Map<String, DataLocation> inputLocations = new HashMap<>(); //maps each input-location to a DataLocation object which specifies it's properties

  @Override
  void execute() {
    String[] loc = getCl().getListExtTblLocsParams();
    try{
      generateExternalTableInfo(loc[0], loc[1]);
    } catch (IOException | TException | JSONException e) {
      System.out.println("Generating external table locations failed: \n" + e.getMessage());
    }
  }

  private void generateExternalTableInfo(String dbPattern, String outputDir) throws TException, IOException,
          JSONException {
    ObjectStore objectStore = getObjectStore();
    conf = msConf != null ? msConf : objectStore.getConf();
    Warehouse wh = new Warehouse(conf);
    String defaultCatalog = MetaStoreUtils.getDefaultCatalog(conf);
    List<String> databases = objectStore.getDatabases(defaultCatalog, dbPattern);
    System.out.println("Number of databases found for given pattern: " + databases.size());
    //maintain the set of leaves of the tree as a sorted set
    Set<String> leafLocations = new TreeSet<>();
    for (String db : databases) {
      List<String> tables = objectStore.getAllTables(defaultCatalog, db);
      Path defaultDbExtPath = wh.getDefaultExternalDatabasePath(db);
      String defaultDbExtLocation = defaultDbExtPath.toString();
      boolean isDefaultPathEmpty = true;
      for(String tblName : tables) {
        Table t = objectStore.getTable(defaultCatalog, db, tblName);
        if(TableType.EXTERNAL_TABLE.name().equalsIgnoreCase(t.getTableType())) {
          String tblLocation = t.getSd().getLocation();
          Path tblPath = new Path(tblLocation);
          if(isPathWithinSubtree(tblPath, defaultDbExtPath)) {
            if(isDefaultPathEmpty) {
              isDefaultPathEmpty = false;
              //default paths should always be included, so we add them as special leaves to the tree
              addDefaultPath(defaultDbExtLocation, db);
              leafLocations.add(defaultDbExtLocation);
            }
            HashSet<String> coveredByDefault = coverageList.get(defaultDbExtLocation);
            coveredByDefault.add(tblLocation);
          } else if (!isCovered(leafLocations, tblPath)) {
            leafLocations.add(tblLocation);
          }
          DataLocation dataLocation = new DataLocation(db, tblName, 0, 0,
                  null);
          inputLocations.put(tblLocation, dataLocation);
          dataLocation.setSizeExtTblData(getDataSize(tblPath, conf));
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
              Path partPath = new Path(partLocation);
              long partDataSize = getDataSize(partPath, conf);
              if (isPathWithinSubtree(partPath, defaultDbExtPath)) {
                if (isDefaultPathEmpty) {
                  isDefaultPathEmpty = false;
                  addDefaultPath(defaultDbExtLocation, db);
                  leafLocations.add(defaultDbExtLocation);
                }
                if (isPathWithinSubtree(partPath, tblPath)) {
                  //even in non-null case, handle the corner case where location is set to table-location
                  //In this case, partition would be covered by table location itself, so we need not add to input
                  dataLocation.incrementNumPartsInTblLoc();
                } else {
                  DataLocation partObj = new DataLocation(db, tblName, 0, 0, partitionName);
                  partObj.setSizeExtTblData(partDataSize);
                  inputLocations.put(partLocation, partObj);
                  coverageList.get(defaultDbExtLocation).add(partLocation);
                }
              } else {
                if (isPathWithinSubtree(partPath, tblPath)) {
                  dataLocation.incrementNumPartsInTblLoc();
                } else {
                  //only in this case, partition location is neither inside table nor in default location.
                  //So we add it to the graph  as a separate leaf.
                  DataLocation partObj = new DataLocation(db, tblName, 0, 0, partitionName);
                  partObj.setSizeExtTblData(partDataSize);
                  inputLocations.put(partLocation, partObj);
                  if(!isCovered(leafLocations, partPath)) {
                    leafLocations.add(partLocation);
                  }
                }
              }
            }
          }
        }
      }
    }
    if(!leafLocations.isEmpty()) {
      removeNestedStructure(leafLocations);
      createOutputList(leafLocations, outputDir, dbPattern);
    }
    else {
      System.out.println("No external tables found to process.");
    }
  }

  private void addDefaultPath(String defaultDbExtLocation, String dbName) {
    coverageList.put(defaultDbExtLocation, new HashSet<>());
    DataLocation defaultDatalocation = new DataLocation(dbName, null, 0, 0, null);
    //mark default leaves to always be included in output-list
    defaultDatalocation.setIncludeByDefault(true);
    inputLocations.put(defaultDbExtLocation, defaultDatalocation);
  }

  private long getDataSize(Path location, Configuration conf) throws IOException {
    if(location == null) {
      return 0;
    }
    if(MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST)) {
      return testDatasizes == null ? 0 : testDatasizes.containsKey(location.toString()) ?
              testDatasizes.get(location.toString()) : 0;
    }
    FileSystem fs = location.getFileSystem(conf);
    if (fs != null && fs.getUri().getScheme().equals("hdfs")) {
      try {
        ContentSummary cs = fs.getContentSummary(location);
        return cs.getLength();
      } catch (FileNotFoundException e) {
        //no data yet in data location but we proceed since data may be added later.
      }
    }
    return 0;
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
  private boolean isCovered(Set<String> locations, Path path) {
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
    //If the path to be covered should be included by default, then we do not cover it.
    //This is because default paths should be individually listed, not covered under some parent.
    if(inputLocations.containsKey(childLoc) && inputLocations.get(childLoc).shouldIncludeByDefault()) {
      return;
    }
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
    }
  }

  /*
   * Transforms a collection so that no element is an ancestor of another.
   */
  private void removeNestedStructure(Set<String> locations) {
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
  private void createOutputList(Set<String> locations, String outputDir, String dbPattern) throws IOException, JSONException {
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
      boolean processChildrenByDefault = false;
      for(ExternalTableGraphNode child : childNodes) {
        if (child.getNumLeavesCovered() > 1) {
          nonTrivialCoverage += child.getNumLeavesCovered();
        }
        if (child.shouldIncludeByDefault()) {
          processChildrenByDefault = true;
          break;
        }
      }
      boolean addCurrToSolution = false;
      if(!processChildrenByDefault) {
        addCurrToSolution = true;
        if (!current.shouldIncludeByDefault()) {
          //ensure that we do not have extra data in the current node for it to be included.
          long currDataSize = getDataSize(new Path(current.getLocation()), conf);
          int numLeavesCovered = current.getNumLeavesCovered();
          //only add current node if it doesn't have extra data and non-trivial coverage is less than half.
          //Also we do not add current node if there is just a single path(numLeavesCovered = 1); in this case we proceed to the leaf.
          addCurrToSolution &= currDataSize == current.getChildDataSizes() &&
                  ((nonTrivialCoverage < (numLeavesCovered + 1) / 2) && numLeavesCovered != 1);
        }
      }
      if(processChildrenByDefault) {
        queue.addAll(childNodes);
      } else if (addCurrToSolution) {
        addToSolution(current);
      } else {
        queue.addAll(childNodes);
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
    String result = jsonObject.toString(4).replace("\\","");
    pw.println(result);
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
      String tblName = data.getTblName();
      if(tblName == null) {
        continue;
      }
      String out = data.getDbName() + "." + tblName;
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

  private ExternalTableGraphNode constructTree(Set<String> locations) {
    ExternalTableGraphNode rootNode = null;
    Map<String, ExternalTableGraphNode> locationGraph = new HashMap<>();
    // Every location is represented by a leaf in the tree.
    // We traverse through the input locations and construct the tree.
    for (String leaf : locations) {
      ExternalTableGraphNode currNode = new ExternalTableGraphNode(leaf, new ArrayList<>(), true, 0);
      if(inputLocations.containsKey(leaf)) {
        if(inputLocations.get(leaf).shouldIncludeByDefault()) {
          currNode.setIncludeByDefault(true);
        }
        currNode.setDataSize(inputLocations.get(leaf).getSizeExtTblData());
      }
      locationGraph.put(leaf, currNode);
      //initialize coverage-lists of leaves
      if (coverageList.get(leaf) == null) {
        coverageList.put(leaf, new HashSet<>());
      }
      //mark the leaf as being covered by itself
      HashSet currCoverage = coverageList.get(leaf);
      currCoverage.add(leaf);
      //set the number of leaves covered. Nested locations could have been covered earlier during preprocessing,
      //so we set it to the size of it's coverage set.
      currNode.setNumLeavesCovered(currCoverage.size());
      Path parent = new Path(leaf).getParent();
      ExternalTableGraphNode parNode;
      //traverse upward to the root in order to construct the graph
      while (parent != null) {
        String parentLoc = parent.toString();
        if (!locationGraph.containsKey(parentLoc)) {
          //if parent doesn't exist in graph then create it
          parNode = new ExternalTableGraphNode(parentLoc, new ArrayList<>(), false, 0);
          locationGraph.put(parentLoc, parNode);
        }
        else {
          parNode = locationGraph.get(parentLoc);
          parNode.setIsLeaf(false);
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
    rootNode.updateIncludeByDefault();
    rootNode.updateDataSize();
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

  @VisibleForTesting
  Map<String, Long> testDatasizes = null;

  @VisibleForTesting
  public Map<String, HashSet<String>> runTest(Set<String> inputList, Map<String, Long> sizes)  {
    try {
      conf = msConf;
      testDatasizes = sizes;
      coverageList.clear();
      removeNestedStructure(inputList);
      createOutputList(inputList, "test", "test");
    } catch (Exception e) {
      LOG.error("MetaToolTask failed on ListExtTblLocs test: ", e);
    }
    return coverageList;
  }

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
    // 'sizeExtTblData' stores the size of useful data in a directory.
    // This can be compared with total directory-size to ascertain amount of extra data in it.
    long sizeExtTblData;
    boolean includeByDefault;

    private DataLocation (String dbName, String tblName, int totalPartitions, int numPartitionsInTblLoc,
                          String partName) {
      this.dbName = dbName;
      this.tblName = tblName;
      this.totalPartitions = totalPartitions;
      this.numPartitionsInTblLoc = numPartitionsInTblLoc;
      this.partName = partName;
      this.sizeExtTblData = 0;
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
    
    private long getSizeExtTblData() {
      return this.sizeExtTblData;
    }

    private boolean shouldIncludeByDefault() {
      return this.includeByDefault;
    }

    private void setTotalPartitions(int totalPartitions) {
      this.totalPartitions = totalPartitions;
    }

    private void setSizeExtTblData(long sizeExtTblData) {
      this.sizeExtTblData = sizeExtTblData;
    }

    private void setIncludeByDefault(boolean includeByDefault) {
      this.includeByDefault = includeByDefault;
    }
  }

  private class ExternalTableGraphNode {
    private String location;
    private List<ExternalTableGraphNode> childNodes;
    private ExternalTableGraphNode parent;
    private boolean isLeaf;
    private boolean includeByDefault;
    private int numLeavesCovered;
    private long dataSize;

    private ExternalTableGraphNode(String location, List<ExternalTableGraphNode> childNodes, boolean isLeaf, long dataSize) {
      this.location = location;
      this.childNodes = childNodes;
      this.isLeaf = isLeaf;
      this.parent = null;
      this.includeByDefault = false;
      this.dataSize = dataSize;
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

    public void setIsLeaf(boolean isLeaf) {
      this.isLeaf = isLeaf;
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

    private boolean shouldIncludeByDefault() {
      return this.includeByDefault;
    }

    private void setIncludeByDefault(boolean includeByDefault) {
      this.includeByDefault = includeByDefault;
    }

    private void setDataSize(long dataSize) {
      this.dataSize = dataSize;
    }

    private long getDataSize() {
      return this.dataSize;
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

    /*
     * Method to mark all the paths in the subtree rooted at current node which need to be included by default.
     * If some leaf has this property, then we mark the path from root to that leaf.
     */
    private void updateIncludeByDefault() {
      if(this.isLeaf) {
        return;
      }
      for(ExternalTableGraphNode currChild : childNodes) {
        currChild.updateIncludeByDefault();
      }
      for(ExternalTableGraphNode currChild : childNodes) {
        if(currChild.shouldIncludeByDefault()) {
          this.includeByDefault = true;
          break;
        }
      }
    }

    /*
     * Method to update the datasize of subtree rooted at a particular node recursively.
     */
    private void updateDataSize() {
      if(this.isLeaf) {
        return;
      }
      for(ExternalTableGraphNode currChild : childNodes) {
        currChild.updateDataSize();
      }
      this.dataSize += this.getChildDataSizes();
    }

    /*
     * Method to return sum of data-sizes of child nodes of a particular node
     */
    private long getChildDataSizes() {
      long sumChildDataSizes = 0;
      for(ExternalTableGraphNode currChild : childNodes) {
        sumChildDataSizes += currChild.getDataSize();
      }
      return sumChildDataSizes;
    }
  }
}
