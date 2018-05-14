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
import Select from 'react-select';
import ReactCodemirror from 'react-codemirror';
import CodeMirror from 'codemirror';
import 'codemirror/mode/javascript/javascript';
import jsonlint from 'jsonlint';
import lint from 'codemirror/addon/lint/lint';
import '../../../utils/Overrides';
import Utils from '../../../utils/Utils';
import FSReactToastr from '../../../components/FSReactToastr';
import CommonNotification from '../../../utils/CommonNotification';
import {toastOpt} from '../../../utils/Constants';
import SchemaREST from '../../../rest/SchemaREST';

export default class SchemaFormContainer extends Component {
  constructor(props) {
    super(props);
    this.state = {
      name: '',
      compatibility: 'BACKWARD',
      compatibilityArr: [
        {
          value: 'BACKWARD',
          label: 'BACKWARD'
        }, {
          value: 'FORWARD',
          label: 'FORWARD'
        }, {
          value: 'BOTH',
          label: 'BOTH'
        }, {
          value: 'NONE',
          label: 'NONE'
        }
      ],
      evolve: true,
      schemaText: '',
      schemaTextFile: null,
      type: 'avro',
      typeArr: [],
      schemaGroup: 'Kafka',
      description: '',
      showError: false,
      changedFields: [],
      showCodemirror: false
    };
    this.fetchData();
  }

  componentDidMount(){
    this.setCodemirroSize();
  }

  componentDidUpdate(){
    this.setCodemirroSize();
  }

  setCodemirroSize(){
    const {JSONCodemirror, formLeftPanel, browseFileContainer} = this.refs;
    const {expandCodemirror} = this.state;
    const height = formLeftPanel.clientHeight - 50;
    if(JSONCodemirror){
      if(!expandCodemirror){
        JSONCodemirror.codeMirror.setSize('100%', height);
      }else{
        JSONCodemirror.codeMirror.setSize('100%', '450px');
      }
    }else{
      browseFileContainer.style.height = height+'px';
    }
  }

  fetchData() {
    SchemaREST.getSchemaProviders().then((results) => {
      this.setState({typeArr: results.entities});
    }).catch(Utils.showError);
  }

  handleValueChange(e) {
    let obj = {};
    obj[e.target.name] = e.target.value;
    this.setState(obj);
  }

  handleTypeChange(obj) {
    if (obj) {
      this.setState({type: obj.type});
    } else {
      this.setState({type: ''});
    }
  }

  handleCompatibilityChange(obj) {
    if (obj) {
      this.setState({compatibility: obj.value});
    } else {
      this.setState({compatibility: ''});
    }
  }

  handleJSONChange(json){
    this.setState({
      schemaText: json
    });
  }

  handleOnDrop(e) {
    e.preventDefault();
    e.stopPropagation();
    if (!e.dataTransfer.files.length) {
      this.setState({schemaTextFile: null});
    } else {
      var file = e.dataTransfer.files[0];
      var reader = new FileReader();
      reader.onload = function(e) {
        if(this.state.type.toLowerCase() == 'avro' && Utils.isValidJson(reader.result)) {
          this.setState({schemaTextFile: file, schemaText: reader.result, showCodemirror: true});
        } else if(this.state.type.toLowerCase() != 'avro') {
          this.setState({schemaTextFile: file, schemaText: reader.result, showCodemirror: true});
        } else {
          this.setState({schemaTextFile: null, schemaText: '', showCodemirror: true});
        }
      }.bind(this);
      reader.readAsText(file);
    }
  }

  handleBrowseFile(e){
    e.preventDefault();
    e.stopPropagation();

    var file = e.target.files[0];
    var reader = new FileReader();
    reader.onload = function(e) {
      this.setState({schemaTextFile: file, schemaText: reader.result, showCodemirror: true});
    }.bind(this);
    reader.readAsText(file);
  }

  handleToggleEvolve(e) {
    this.setState({evolve: e.target.checked});
  }

  validateData() {
    let {name, type, schemaGroup, description, changedFields, schemaText} = this.state;
    if (name.trim() === '' || schemaGroup === '' || type === '' || description.trim() === '' || schemaText.trim() === '') {
      if (name.trim() === '' && changedFields.indexOf("name") === -1) {
        changedFields.push("name");
      };
      if (schemaGroup.trim() === '' && changedFields.indexOf("schemaGroup") === -1) {
        changedFields.push("schemaGroup");
      }
      if (type.trim() === '' && changedFields.indexOf("type") === -1) {
        changedFields.push("type");
      }
      if (description.trim() === '' && changedFields.indexOf("description") === -1) {
        changedFields.push("description");
      }
      this.setState({showError: true, changedFields: changedFields, expandCodemirror: false});
      return false;
    } else if (this.state.type.toLowerCase() == 'avro' && !Utils.isValidJson(schemaText.trim())) {/*Add validation logic to Utils method for schema type other than "Avro" */
      return false;
    } else {
      this.setState({showError: false});
      return true;
    }
  }

  handleSave(didVersionFailed) {
    let data = {};
    let {name, type, schemaGroup, description, compatibility, evolve, schemaText} = this.state;
    data = {
      name,
      type,
      schemaGroup,
      description,
      evolve
    };
    let versionData = { schemaText, description };
    if (compatibility !== '') {
      data.compatibility = compatibility;
    }
    if(didVersionFailed){
      return SchemaREST.postVersion(name, {body: JSON.stringify(versionData)});
    } else {
      return SchemaREST.postSchema({body: JSON.stringify(data)})
        .then((schemaResult)=>{
          if(schemaResult.responseMessage !== undefined){
            FSReactToastr.error(<CommonNotification flag="error" content={schemaResult.responseMessage}/>, '', toastOpt);
          } else {
            return SchemaREST.postVersion(name, {body: JSON.stringify(versionData)});
          }
        });
    }
  }

  render() {
    const jsonoptions = {
      lineNumbers: true,
      mode: "application/json",
      styleActiveLine: true,
      gutters: this.state.type.toLowerCase() == 'avro' ? ["CodeMirror-lint-markers"] : [],
      lint: this.state.type.toLowerCase() == 'avro'
    };
    let {evolve, schemaText, showError, changedFields, showCodemirror, expandCodemirror} = this.state;
    return (
      <form>
      <div className="row">
        <div className={expandCodemirror ? "hidden" : "col-md-6"} ref="formLeftPanel">
          <div className="form-group">
            <label>Name <span className="text-danger">*</span></label>
            <div>
              <input name="name" placeholder="Name" onChange={this.handleValueChange.bind(this)} type="text" className={showError && changedFields.indexOf("name") !== -1 && this.state.name.trim() === ''
                ? "form-control invalidInput"
                : "form-control"} value={this.state.name} required={true}/>
            </div>
          </div>
          <div className="form-group">
            <label>description <span className="text-danger">*</span></label>
            <div>
              <input name="description" placeholder="Description" onChange={this.handleValueChange.bind(this)} type="text" className={showError && changedFields.indexOf("description") !== -1 && this.state.description.trim() === ''
                ? "form-control invalidInput"
                : "form-control"} value={this.state.description} required={true}/>
            </div>
          </div>
          <div className="form-group">
            <label>Type <span className="text-danger">*</span></label>
            <div>
              <Select value={this.state.type} options={this.state.typeArr} onChange={this.handleTypeChange.bind(this)} valueKey="type" labelKey="name"/>
            </div>
          </div>
          <div className="form-group">
            <label>Schema Group <span className="text-danger">*</span></label>
            <div>
              <input name="schemaGroup" placeholder="Schema Group" onChange={this.handleValueChange.bind(this)} type="text" className={showError && changedFields.indexOf("schemaGroup") !== -1 && this.state.schemaGroup === ''
                ? "form-control invalidInput"
                : "form-control"} value={this.state.schemaGroup} required={true}/>
            </div>
          </div>
          <div className="form-group">
            <label>Compatibility</label>
            <div>
              <Select value={this.state.compatibility} options={this.state.compatibilityArr} onChange={this.handleCompatibilityChange.bind(this)} clearable={false}/>
            </div>
          </div>
          <div className="form-group">
            <div className="checkbox">
            <label><input name="evolve" onChange={this.handleToggleEvolve.bind(this)} type="checkbox" value={evolve} checked={evolve}/> Evolve</label>
            {!evolve
              ?
              <span className="checkbox-warning"> <i className="fa fa-exclamation-triangle" aria-hidden="true"></i> New schema versions cannot be added.</span>
              :
              null
            }
          </div>
        </div>
      </div>
      <div className={expandCodemirror ? "col-md-12" : "col-md-6"}>
        <div className="form-group">
          <label>
            Schema Text <span className="text-danger">*</span>
          </label>
          {showCodemirror
            ? [<a key="1" className="pull-right clear-link" href="javascript:void(0)" onClick={() => { this.setState({schemaText: '', showCodemirror: false, expandCodemirror: false}); }}> CLEAR </a>,
              <span key="2" className="pull-right" style={{margin: '-1px 5px 0'}}>|</span>,
              <a key="3" className="pull-right" href="javascript:void(0)" onClick={() => { this.setState({expandCodemirror: !expandCodemirror}); }}>
                {expandCodemirror ? <i className="fa fa-compress"></i> : <i className="fa fa-expand"></i>}
              </a>]
            :
            null
          }
          <div onDrop={this.handleOnDrop.bind(this)} onDragOver={(e) => {
            e.preventDefault();
            e.stopPropagation();
            return false;
          }}
          >
            {showCodemirror
              ?
              <ReactCodemirror ref="JSONCodemirror" value={this.state.schemaText} onChange={this.handleJSONChange.bind(this)} options={jsonoptions} />
              :
              <div ref="browseFileContainer" className={"addSchemaBrowseFileContainer"+(showError && this.state.type.toLowerCase() == 'avro' && !Utils.isValidJson(schemaText) ? ' invalidInput' : '')}>
                <div onClick={(e) => {
                  this.setState({showCodemirror: true});
                }}>
                  <div className="main-title">Copy & Paste</div>
                  <div className="sub-title m-t-sm m-b-sm">OR</div>
                  <div className="main-title">Drag & Drop</div>
                  <div className="sub-title" style={{"marginTop": "-4px"}}>Files Here</div>
                  <div className="sub-title m-t-sm m-b-sm">OR</div>
                  <div  className="m-t-md">
                    <input type="file" ref="browseFile" className="inputfile" onClick={(e) => {
                      e.stopPropagation();
                    }} onChange={this.handleBrowseFile.bind(this)}/>
                    <label htmlFor="file" className="btn btn-success">BROWSE</label>
                  </div>
                </div>
              </div>
            }
          </div>
        </div>
      </div>
      </div>
      </form>
    );
  }
}
