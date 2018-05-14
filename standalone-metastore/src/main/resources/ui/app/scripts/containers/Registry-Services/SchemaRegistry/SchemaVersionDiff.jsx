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
import ReactDOM from 'react-dom';
import _ from 'lodash';
import Select from 'react-select';
import {Button, ButtonGroup} from 'react-bootstrap';
import Utils from '../../../utils/Utils';
import {diffview} from '../../../libs/jsdifflib/diffview';
import {difflib} from '../../../libs/jsdifflib/difflib';

export default class SchemaVersionDiff extends Component {
  constructor(props) {
    super(props);
    let versionsArr = Utils.sortArray(this.props.schemaObj.schemaVersionInfos.slice(), 'version', false);
    let newTextOptions = versionsArr;
    let baseTextOptions = versionsArr;
    this.state = {
      baseText: versionsArr[1].schemaText,
      newText: versionsArr[0].schemaText,
      baseTextVersion: versionsArr[1],
      baseTextOptions: baseTextOptions,
      newTextVersion: versionsArr[0],
      newTextOptions: newTextOptions,
      diffOutputText: '',
      versionsArr: _.clone(versionsArr),
      inlineView: true
    };
  }

  componentDidUpdate() {
    this.getSchemaDiff();
  }

  handleBaseVersionChange(obj) {
    let {versionsArr, newTextVersion, newText, baseTextOptions} = this.state;
    if(obj.version) {
      let s = _.find(baseTextOptions, {version: obj.version});
      this.setState({baseText: s.schemaText, baseTextVersion: s});
    }
  }
  handleNewVersionChange(obj) {
    if(obj.version) {
      let {newTextOptions, baseTextVersion, baseText, versionsArr} = this.state;
      let s = _.find(newTextOptions, {version: obj.version});
      this.setState({newText: s.schemaText, newTextVersion: s});
    }
  }
  toggleDiffView(e) {
    this.setState({
      inlineView: e.target.dataset.name === "inlineDiff" ? true : false
    });
  }
  getSchemaDiff(){
    let {baseText, newText, baseTextVersion, newTextVersion, inlineView} = this.state;
    // get the baseText and newText values and split them into lines
    var base = difflib.stringAsLines(baseText);
    var newtxt = difflib.stringAsLines(newText);
    // create a SequenceMatcher instance that diffs the two sets of lines
    var sm = new difflib.SequenceMatcher(base, newtxt);
    // get the opcodes from the SequenceMatcher instance
    // opcodes is a list of 3-tuples describing what changes should be made to the base text
    // in order to yield the new text
    var opcodes = sm.get_opcodes();

    var diffoutputdiv = document.getElementsByClassName('diffoutputdiv')[0];
    while (diffoutputdiv.firstChild) {diffoutputdiv.removeChild(diffoutputdiv.firstChild);}

    // build the diff view and add it to the current DOM
    diffoutputdiv.appendChild(diffview.buildView({
      baseTextLines: base,
      newTextLines: newtxt,
      opcodes: opcodes,
      baseTextName: baseTextVersion ? ("V " + baseTextVersion.version) : '',
      newTextName: newTextVersion ?  ("V " + newTextVersion.version) : '',
      viewType: inlineView ? 1 : 0
    }));

  }

  render() {
    let {baseTextVersion, newTextVersion, baseTextOptions, newTextOptions, baseText, newText, inlineView} = this.state;

    return (
        <div>
      <div className="form-horizontal">
        <div className="form-group version-diff-labels m-b-sm">
            <label className="control-label col-sm-2">Base Version</label>
            <div className="col-sm-2">
              <Select value={baseTextVersion} options={baseTextOptions} onChange={this.handleBaseVersionChange.bind(this)} valueKey="version" labelKey="version" clearable={false} />
            </div>
            <label className="control-label col-sm-offset-2 col-sm-2">New Version</label>
            <div className="col-sm-2">
              <Select value={newTextVersion} options={newTextOptions} onChange={this.handleNewVersionChange.bind(this)} valueKey="version" labelKey="version" clearable={false} />
            </div>
        </div>
        <div className="row">
          <div className="col-sm-6">
            <label className="bold">Description</label>
            <p className="version-description">{baseTextVersion ? baseTextVersion.description : ''}</p>
          </div>
          <div className="col-sm-6">
            <label className="bold">Description</label>
            <p className="version-description">{newTextVersion ? newTextVersion.description : ''}</p>
          </div>
        </div>
      <hr />
      <div className="row">
        <div className="col-sm-12">
          {inlineView ?
          (<div className="table-header-custom unified">{"V " + (baseTextVersion ? baseTextVersion.version : '') + " vs. V " + (newTextVersion ? newTextVersion.version : '')}
              <ButtonGroup bsClass=" btn-group btn-group-xs pull-right">
                <Button
                  className={inlineView ? "diffViewBtn btn selected" : "diffViewBtn btn"}
                  data-name="inlineDiff"
                  onClick={this.toggleDiffView.bind(this)}
                >
                  Unified
                </Button>
                <Button
                  className={inlineView ? "diffViewBtn btn" : "diffViewBtn btn selected"}
                  data-name="sideBySideDiff"
                  onClick={this.toggleDiffView.bind(this)}
                >
                  Split
                </Button>
            </ButtonGroup>
          </div>)
          :
          (<div className="table-header-custom split">
            <div className="col-sm-6">{"V " + (baseTextVersion ? baseTextVersion.version : '')}</div>
            <div>{"V " + (newTextVersion ? newTextVersion.version : '')}</div>
            <ButtonGroup bsClass="btn-group btn-group-xs pull-right">
                <Button
                  className={inlineView ? "diffViewBtn btn selected" : "diffViewBtn btn"}
                  data-name="inlineDiff"
                  onClick={this.toggleDiffView.bind(this)}
                >
                  Unified
                </Button>
                <Button
                  className={inlineView ? "diffViewBtn btn" : "diffViewBtn btn selected"}
                  data-name="sideBySideDiff"
                  onClick={this.toggleDiffView.bind(this)}
                >
                  Split
                </Button>
            </ButtonGroup>
          </div>)
          }
          <div className="diffoutputdiv"></div>
        </div>
      </div>
      </div>
      </div>
    );
  }
}
