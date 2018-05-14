/**
  * Copyright 2017 Hortonworks.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *   http://www.apache.org/licenses/LICENSE-2.0
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
**/

import React, {Component} from 'react';
import PropTypes from 'prop-types';
import ReactDOM from 'react-dom';
import _ from 'lodash';
import 'gitgraph.js';
import Utils from '../utils/Utils';

export default class SchemaBranches extends Component {
  constructor(props) {
    super(props);
  }
  componentDidMount(){
    const schema = JSON.parse(JSON.stringify(this.props.schema));
    const graphData= [];

    schema.schemaBranches.forEach((branch, index)=>{
      var sortedVersions = branch.schemaVersionInfos.sort((a, b) => {
        return a.version > b.version;
      });
      sortedVersions.forEach((ver, i) => {
        if(branch.schemaBranch.name !== 'MASTER' && i == 0) {
          ver.hideVersion = true;
        }
        ver.branch = branch;
        graphData.push(ver);
      });
    });

    const sortedByTS = graphData.sort((a, b) => {
      if(a.timestamp == b.timestamp){
        return a.branch.schemaBranch.timestamp - b.branch.schemaBranch.timestamp;
      } else{
        return a.timestamp - b.timestamp;
      }
    });

    const templateConfig = {
      colors: [ "#1290c0", "#8ec92f", "#f06261", "#d05b36", "#f5d73a" ], // branches colors, 1 per column
      branch: {
        lineWidth: 4,
        spacingX: 50,
        showLabel: true // display branch names on graph
      },
      commit: {
        spacingY: -50,
        dot: {
          size: 7
        },
        message: {
          displayAuthor: true,
          displayBranch: true,
          displayHash: false,
          font: "normal 12pt Open Sans"
        },
        shouldDisplayTooltipsInCompactMode: true,
        tooltipHTMLFormatter: function ( commit ) {
          return commit.message;
        }
      }
    };
    const template = new GitGraph.Template( templateConfig );

    const graph = new GitGraph({
      elementId: schema.schemaMetadata.name+'Graph',
      template: template,
      orientation: "horizontal",
      mode: "compact"
    });

    const branches = {};
    sortedByTS.forEach((ver) => {
      const branchName = ver.branch.schemaBranch.name;
      const branch = branches[branchName];
      if(!branch){
        let parentBranch;
        const rootSchemaVersion = ver.branch.rootSchemaVersion;
        if(rootSchemaVersion == null){
          branches.MASTER = branches.MASTER || graph.branch('master');
          parentBranch = branches.MASTER;
        }else{
          const _ver = _.find(sortedByTS, (_ver) => {
            return _ver != ver && _ver.id == rootSchemaVersion;
          });
          parentBranch = branches[_ver.branch.schemaBranch.name] || graph.branch(_ver.branch.schemaBranch.name);
        }
        branches[branchName] = parentBranch.branch(branchName);
      }
      if(ver.mergeInfo != null && ver.hideVersion !== true) {
        let mergeInfo = ver.mergeInfo;
        let schemaBranch = mergeInfo.schemaBranchName;
        branches[schemaBranch].merge(branches.MASTER, {
          message: 'v'+ver.version + ' created on '+ Utils.getDateTimeLabel(new Date(ver.timestamp))
        });
      } else if(ver.hideVersion) {
        branches[branchName].commit({
          dotSize: 0.001,
          tooltipDisplay: false
        });
      } else {
        branches[branchName].commit({
          message: 'v' + ver.version + ' created on ' + Utils.getDateTimeLabel(new Date(ver.timestamp))
        });
      }
    });
  }

  render(){
    const {schema} = this.props;
    return (
      <canvas id={schema.schemaMetadata.name+'Graph'}></canvas>
    );
  }
}