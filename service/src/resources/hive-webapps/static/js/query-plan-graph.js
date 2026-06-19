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
/**
* query-plan-graph.js
*
* Display a visualization of query plan
*/

const DEFAULT_TEXT_INDENT = 2;
const MAX_ORDER_TRIES = 100;

const SUCCESS = "Success";
const RUNNING = "Running";
const FAILED = "Failure";

const PROGRESS_ELEMENT = "progress-bar-element";
const LONGEST_PATH_FROM_ROOT = "longestPathFromRoot";
const NODE_ID = "nodeId";

const BLUE = {
  border:'#2B7CE9',
  background : '#D2E5FF',
  highlight : {
  border : '#2B7CE9',
  background : '#D2E5FF'
  }
}
const GREEN = {
  border : '#009900',
  background : '#99E699',
  highlight : {
  border : '#009900',
    background : '#99E699'
  }
}
const PINK = 'pink';
const LIGHT_GRAY = 'rgba(200,200,200,0.5)';


/**
* Displays query plan as a graph as well as information about a selected stage.
*/
function visualizeJsonPlan(displayGraphElement, displayStagePlanElement, displayStatisticsElement,
  displayStatisticsElementHead, displayStatisticsElementBody, jsonPlan, jsonStatuses, jsonStats,
  jsonLogs, displayInformationTextIndent = DEFAULT_TEXT_INDENT) {

  setUpInstructions(displayStagePlanElement, displayStatisticsElementHead);
  networkData = getNodesAndEdges(jsonPlan["STAGE DEPENDENCIES"]);
  network = createNetwork(networkData.nodes, networkData.edges, displayGraphElement);
  network.on('click', function (params) {
    displayStagePlan(params, displayStagePlanElement, jsonPlan, displayInformationTextIndent);
    if (document.getElementById(displayStatisticsElement) != null) {
      displayStatistics(params, displayStatisticsElement, displayStatisticsElementHead,
        displayStatisticsElementBody, jsonStats, jsonStatuses, jsonLogs,
        displayInformationTextIndent);
    }
  });
  colorTasks(networkData.edges, networkData.nodes, jsonStatuses)
  return network;
}


function setUpInstructions(displayStagePlanElement, displayStatisticsElementHead) {
  document.getElementById(displayStagePlanElement).innerHTML =
    "Click on a stage to view its plan";
  if (document.getElementById(displayStatisticsElementHead) != null) {
    document.getElementById(displayStatisticsElementHead).innerHTML =
      "Click on a colored-in stage to view stats";
  }
}

function colorTasks(edges, nodes, jsonStatuses) {
  grayFilteredOutTasks(edges, nodes, jsonStatuses);
  showSuccessfulTasks(edges, nodes, jsonStatuses);
  showRunningTasks(nodes, jsonStatuses);
  showFailedTasks(nodes, jsonStatuses);
  }

/**
* Set color of node or edge
*/
function setColor(dataSet, itemToChange, newColor) {
  itemToChange.color = newColor;
  dataSet.update(itemToChange);
}


/**
* Colors all edges connected to the node.
*/
function setAllNodeEdgesColor(edgesDataSet, nodeId, newColor) {
  var theseEdges = edgesDataSet.get({
    filter: function(item) {
      return (item.to == nodeId || item.from == nodeId);
    }});
  for (var i = theseEdges.length - 1; i >= 0; i--) {
    setColor(edgesDataSet, theseEdges[i], newColor);
  }
}


/**
* If two nodes are the same color, change the color of their connecting edge to match
*/
function matchAllEdgeColors(nodesDataSet, edgesDataSet) {
  for (node in nodesDataSet.getIds()) {
      nodeId = nodesDataSet.getIds()[node];
    var toEdges = edgesDataSet.get({
      filter: function (item) {
        return item.to == nodeId;
      }});
    for (var i = toEdges.length - 1; i >= 0; i--) {
      if (nodesDataSet.get(nodeId).color == nodesDataSet.get(parseInt(toEdges[i].from)).color) {
        setColor(edgesDataSet, toEdges[i], nodesDataSet.get(nodeId).color.border);
      }
    }
  }
}


/**
* If a task is not filtered out by conditional statements, it will be in jsonStatuses.
* Set all other tasks' node & edge colors to light gray
*/
function grayFilteredOutTasks(edgesDataSet, nodesDataSet, jsonStatuses) {

  for (node in nodesDataSet.getIds()) {
    nodeId = nodesDataSet.getIds()[node];
    var taskId = "Stage-" + nodeId;
    var otherTaskId = nodesDataSet.get(nodeId).id;
    if (!Object.keys(jsonStatuses).includes(taskId)) {
      setColor(nodesDataSet, nodesDataSet.get(nodeId), LIGHT_GRAY);
      setAllNodeEdgesColor(edgesDataSet, nodeId, LIGHT_GRAY);
    }
  }
}


/**
* If a task is successful, color it green!
*/
function showSuccessfulTasks(edgesDataSet, nodesDataSet, jsonStatuses) {
  changeNodeColorByStatus(nodesDataSet, jsonStatuses, SUCCESS, GREEN)
  matchAllEdgeColors(nodesDataSet, edgesDataSet);
}

function showRunningTasks(nodesDataSet, jsonStatuses) {
  changeNodeColorByStatus(nodesDataSet, jsonStatuses, RUNNING, BLUE)
}

function showFailedTasks(nodesDataSet, jsonStatuses) {
  changeNodeColorByStatus(nodesDataSet, jsonStatuses, FAILED, PINK)
}

function changeNodeColorByStatus(nodesDataSet, jsonStatuses, statusString, color) {
  for (node in nodesDataSet.getIds()) {
    nodeId = nodesDataSet.getIds()[node];
    var taskId = "Stage-" + nodeId;
    if (Object.keys(jsonStatuses).includes(taskId)) {
      if (jsonStatuses[taskId].search(statusString) != -1) {
        setColor(nodesDataSet, nodesDataSet.get(nodeId), color);
      }
    }
  }
}


/**
* Removes all non-number characters from string, casts to integer
* @param {Object} stageDependencies - json object containing node and edge information
* returns {nodes, edges} - javascript arrayObjects containing node, edge info readable by vis.js
*/
function getNodesAndEdges(stageDependencies) {
  var nodes = [];
  var edges = [];
  var paths = [];
  var childNodes = [];
  var nodeIndex = 0;
  var edgeIndex = 0;
  var newNode = {};
  var newEdge = {};
  var nodeId = 0;
  var newLabel = "";
  for (stage in stageDependencies) {
    newNode = {};
    nodeId = getStageNumber(stage);
    newNode['id'] = nodeId;
    newLabel = nodeId + " - " + stageDependencies[stage]["TASK TYPE"];
    if (stageDependencies[stage]["ROOT STAGE"] == "TRUE") {
      newLabel += " - ROOT";
      //create new path
      paths.push([nodeId]);
    }
    else {
      childNodes.push(nodeId);
    }
    newNode['label'] = newLabel;
    if (stageDependencies[stage]["CONDITIONAL CHILD TASKS"] != null) {
      newNode['shape'] = 'text';
    }
    nodes[nodeIndex] = newNode;
    edgeIndex = linkStages("DEPENDENT STAGES", stageDependencies, stage, nodeId, edgeIndex, edges);
    edgeIndex = linkStages("CONDITIONAL CHILD TASKS", stageDependencies, stage, nodeId, edgeIndex,
      edges, true);

    nodeIndex++;
  }

  for (var i = paths.length - 1; i >= 0; i--) {
    for (ndx in childNodes) {
      paths[i].push(childNodes[ndx]);
    }
  }
  assignNodeLevel(paths, nodes, edges);

  var nodesDataSet = new vis.DataSet(nodes);
  var edgesDataSet = new vis.DataSet(edges);
  return {
    nodes: nodesDataSet,
    edges: edgesDataSet
  }
}


/**
* Make each node's level in hierarchy longest path from any root
*/
function assignNodeLevel(paths, nodes, edges) {

  orderPathsByEdgeDirection(paths, edges);
  for (pathNdx in paths) {
    prepareMap(paths[pathNdx]);
    assignLongestPathFromRoot(paths[pathNdx], edges)
  }
  assignLevelByLongestPathFromAllRoots(paths, nodes);
}


function orderPathsByEdgeDirection(paths, edges) {
  for (pathNdx in paths) {
    var currentPath = paths[pathNdx];
    var orderedPath = []
    var listOfEdges = edges.slice(0);
    for (var edgeNdx = 0; edgeNdx < listOfEdges.length; edgeNdx++) {
      if (!(currentPath.includes(listOfEdges[edgeNdx].to) && currentPath.includes(listOfEdges[edgeNdx].from))) {
        listOfEdges.splice(edgeNdx, 1);
        edgeNdx--;
      }
    }
    var count = 0;
    while (currentPath.length != 0 && count < MAX_ORDER_TRIES) {
      for (var nodeNdx in currentPath) {
        var node = currentPath[nodeNdx];
        var edgesIn = 0;
        for (edgeNdx in listOfEdges) {
          if (listOfEdges[edgeNdx].to == node) {
            edgesIn++;
            break;
          }
        }
        if (edgesIn == 0) {
          orderedPath.push(node);
          currentPath.splice(nodeNdx, 1);
          for (var edgeNdx = 0; edgeNdx < listOfEdges.length; edgeNdx++) {
            if (listOfEdges[edgeNdx].from == node || listOfEdges[edgeNdx].to == node) {
              listOfEdges.splice(edgeNdx, 1);
              edgeNdx--;
            }
          }
        }
      }
      count++;
    }
    paths[pathNdx] = orderedPath;
  }
}


function prepareMap(currentPath) {
  for (currentPathNdx in currentPath) {
    nodeId = currentPath[currentPathNdx];
    currentPath[currentPathNdx] = {};
    currentPath[currentPathNdx][NODE_ID] = nodeId;
    if (currentPathNdx == 0) {
      currentPath[currentPathNdx][LONGEST_PATH_FROM_ROOT] = 0;
    }
    else {
      currentPath[currentPathNdx][LONGEST_PATH_FROM_ROOT] = -Infinity;
    }
  }
}


function assignLongestPathFromRoot(currentPath, edges) {
  for (var nodeIndex = 1; nodeIndex <= currentPath.length - 1; nodeIndex++) { //skips index 0, root
    var node = currentPath[nodeIndex];
    var nodeId = node[NODE_ID];
    for (var prevNodeIndex = 0; prevNodeIndex < nodeIndex; prevNodeIndex++) {
      var prevNode = currentPath[prevNodeIndex];
      for (edgeNdx in edges) {
        if (edges[edgeNdx].to == nodeId && edges[edgeNdx].from == prevNode.nodeId &&
          currentPath[nodeIndex][LONGEST_PATH_FROM_ROOT] <= prevNode[LONGEST_PATH_FROM_ROOT]) {
          currentPath[nodeIndex][LONGEST_PATH_FROM_ROOT] = prevNode[LONGEST_PATH_FROM_ROOT] + 1;
        }
      }
    }
  }
}


function assignLevelByLongestPathFromAllRoots(paths, nodes) {
  for (nodeNdx in nodes) {
    var nodeId = nodes[nodeNdx].id;
    var longestPath = 0;
    for (pathNdx in paths) {
      currentPath = paths[pathNdx];
      for (pathNodeNdx in currentPath) {
        if (currentPath[pathNodeNdx][NODE_ID] == nodeId &&
          currentPath[pathNodeNdx][LONGEST_PATH_FROM_ROOT] > longestPath) {
          longestPath = currentPath[pathNodeNdx][LONGEST_PATH_FROM_ROOT];
        }
      }
    }
    nodes[nodeNdx]['level'] = longestPath;
  }
}

/**
* Creates a vis.js hierarchical network
* @param {nodes} - vis DataSet, node info
* @param {edges} - vis DataSet, edge info
* @param {documentElement} - where to place the network
*/
function createNetwork(nodes, edges, documentElement) {
  var data = {
    nodes: nodes,
    edges: edges
  };
  var container = document.getElementById(documentElement);
  var options = {
    layout: {
      hierarchical: {
        direction: 'LR',
        sortMethod: 'directed',
        levelSeparation: 150,
        parentCentralization: true
      }
    },
    edges: {
      smooth: true,
      arrows: {to : true }
    },
    physics: {
      hierarchicalRepulsion: {
        nodeDistance: 150
      }
    },
    interaction: {
      zoomView: false
    }
  };
  var network = new vis.Network(container, data, options);
  return network;
}


/**
* Removes all non-number characters from string, casts to integer
* @param {string} - string to mine the number from
* returns {int} - the integer needed from the string
*/
function getStageNumber(string) {
  return parseInt(string.replace( /^\D+/g, ''));
}

function prettifyJsonString(jsonString) {
  return jsonString.replace(/{/g, "").replace(/}/g, "").replace(/,/g, "");
}


function addDashedEdge(parent, child, edgeIndex, edges) {
  addEdge(parent, child, edgeIndex, edges);
  edges[edgeIndex]['dashes'] = 'true';
}


function addEdge(parent, child, edgeIndex, edges) {
  newEdge = {};
  newEdge['from'] = parent;
  newEdge['to'] = child;
  newEdge['color'] = 'gray';
  edges[edgeIndex] = newEdge;
}


/**
* Add edge information
* returns {int} edgeIndex - where we are in the list of edges
*/
function linkStages(linkType, data, stage, nodeId, edgeIndex, edges, dashes=false) {
  if (data[stage][linkType] != null) {
    linkedStages = data[stage][linkType].split(",");
    for (index in linkedStages) {
      if (dashes == true) {
        addDashedEdge(nodeId, getStageNumber(linkedStages[index]), edgeIndex, edges);
      }
      else {
        addEdge(getStageNumber(linkedStages[index]), nodeId, edgeIndex, edges);
      }
      edgeIndex ++
    }
  }
  return edgeIndex;
}


function displayStagePlan(params, displayStagePlanElement, jsonPlan, textIndent) {
  nodeId = params.nodes;
  stageName = "Stage-" + nodeId;
  if (nodeId != "") {
    document.getElementById(displayStagePlanElement).innerHTML =
      'Stage ' + nodeId + " plan:\n" +
      prettifyJsonString(JSON.stringify(jsonPlan['STAGE PLANS'][stageName], null, textIndent));
  }
  //show nothing if no node is selected
  else {
    document.getElementById(displayStagePlanElement).innerHTML =
      "Click on a stage to view its plan";
  }
}


function displayStatistics(params, displayStatisticsElement, displayStatisticsElementHead,
  displayStatisticsElementBody, jsonStats, jsonStatuses, jsonLogs, textIndent) {
  if (document.getElementById(PROGRESS_ELEMENT)) {
    document.getElementById(PROGRESS_ELEMENT).remove();
  }
  nodeId = params.nodes;
  stageName = "Stage-" + nodeId;
  if (nodeId != "" && jsonStatuses[stageName] != null) {
    if (jsonStats[stageName] != null) {

      document.getElementById(displayStatisticsElementHead).innerHTML =
        "Stage " + nodeId + " statistics:\n" +
        "Status: " + jsonStatuses[stageName] + "\n" +
        "Logs: " + prettifyJsonString(JSON.stringify(jsonLogs, null, textIndent)) + "\n\n" +
        "MapReduce job progress:\n";
      document.getElementById(displayStatisticsElementBody).innerHTML =
        prettifyJsonString(JSON.stringify(jsonStats[stageName], null, textIndent));

      if (jsonStats[stageName][MAP_PROGRESS] != null) {
        var mapProgress = jsonStats[stageName][MAP_PROGRESS];
      }
      if (jsonStats[stageName][REDUCE_PROGRESS] != null) {
        var reduceProgress = jsonStats[stageName][REDUCE_PROGRESS];
      }
      var progressBar = getMainProgressBar(mapProgress, reduceProgress, PROGRESS_ELEMENT);
      if (progressBar != null) {
        document.getElementById(displayStatisticsElement).insertBefore(
          progressBar, document.getElementById(displayStatisticsElementBody));
      }
    }
    else { // stage without statistics
      document.getElementById(displayStatisticsElementHead).innerHTML =
        "Stage " + nodeId + " statistics:\n" +
        "Status: " + jsonStatuses[stageName]+ "\n" +
        "Logs: " + prettifyJsonString(JSON.stringify(jsonLogs, null, textIndent));
      document.getElementById(displayStatisticsElementBody).innerHTML = '';
    }
  }
  //show nothing if no node is selected or selected node isn't executed
  else {
    document.getElementById(displayStatisticsElementHead).innerHTML =
      "Click on a colored-in stage to view stats";
    document.getElementById(displayStatisticsElementBody).innerHTML = '';
  }
}


function getMainProgressBar(mapProgress, reduceProgress, elementId) {
  var progressBar = document.createElement("div");
  progressBar.className = "progress";
  progressBar.id = elementId;
  var numChildProgressBars = 2;
  if (mapProgress == null || reduceProgress == null) {
    numChildProgressBars = 1;
  }
  if (mapProgress != null) {
    progressBar.appendChild(getSubProgressBar("map", mapProgress, progressBar,
        numChildProgressBars));
  }
  if (reduceProgress != null) {
    progressBar.appendChild(getSubProgressBar("reduce", reduceProgress, progressBar,
        numChildProgressBars, 'green'));
  }
  if (mapProgress == null && reduceProgress == null) {
    progressBar = null;
  }
  return progressBar;
}


function getSubProgressBar(type, progress, mainProgressBar, numChildProgressBars, color) {
  var new_progress_bar = document.createElement("div");
  new_progress_bar.id = "progress-" + type;
  var className = "progress-bar";
  if (color == "green") {
    className += " progress-bar-success";
  }
  new_progress_bar.className = className;
  new_progress_bar.role = "progressbar";
  new_progress_bar['aria-valuenow'] = "0";
  new_progress_bar['aria-valuemin'] = progress / numChildProgressBars;
  new_progress_bar['aria-valuemax'] = "100";
  new_progress_bar.style = "width: " + progress / numChildProgressBars + "%; min-width: 2em;";
  new_progress_bar.innerHTML = progress + "% " + type;
  return new_progress_bar;
}