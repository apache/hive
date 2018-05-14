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
import ReactCodemirror from 'react-codemirror';
import '../../../utils/Overrides';
import CodeMirror from 'codemirror';
import 'codemirror/mode/javascript/javascript';
import jsonlint from 'jsonlint';
import lint from 'codemirror/addon/lint/lint';
import SchemaREST from '../../../rest/SchemaREST';
import Utils from '../../../utils/Utils';
import {statusCode} from '../../../utils/Constants';

CodeMirror.registerHelper("lint", "json", function(text) {
  var found = [];
  var {parser} = jsonlint;
  parser.parseError = function(str, hash) {
    var loc = hash.loc;
    found.push({
      from: CodeMirror.Pos(loc.first_line - 1, loc.first_column),
      to: CodeMirror.Pos(loc.last_line - 1, loc.last_column),
      message: str
    });
  };
  try {
    jsonlint.parse(text);
  } catch (e) {}
  return found;
});

export default class SchemaVersionForm extends Component {
  constructor(props) {
    super(props);
    let schemaObj = this.props.schemaObj;
    this.state = {
      schemaText: schemaObj.schemaText || '',
      schemaTextFile: schemaObj.schemaTextFile || null,
      description: '',
      showError: false,
      changedFields: [],
      showCodemirror: true,
      schemaTextCompatibility: statusCode.Ok
    };
  }

  handleValueChange(e) {
    let obj = {};
    obj[e.target.name] = e.target.value;
    this.setState(obj);
  }

  handleJSONChange(json) {
    this.setState({schemaText: json});
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
        if(this.props.schemaObj.type.toLowerCase() == 'avro' && Utils.isValidJson(reader.result)) {
          this.setState({schemaTextFile: file, schemaText: reader.result, showCodemirror: true});
        } else if(this.props.schemaObj.type.toLowerCase() != 'avro') {
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

  validateData() {
    let {schemaText, description, changedFields} = this.state;
    if (schemaText.trim() === '' || description.trim() === '') {
      if (description.trim() === '' && changedFields.indexOf("description") === -1) {
        changedFields.push('description');
      }
      this.setState({showError: true, changedFields: changedFields});
      return false;
    } else if(this.props.schemaObj.type.toLowerCase() == 'avro' && !Utils.isValidJson(schemaText.trim())) {/*Add validation logic to Utils method for schema type other than "Avro" */
      return false;
    } else {
      this.setState({showError: false});
      return true;
    }
  }

  handleSave() {
    let {schemaText, description} = this.state;
    let {schemaObj} = this.props;
    let data = {
      schemaText,
      description
    };
    return SchemaREST.getCompatibility(this.props.schemaObj.schemaName, {body: JSON.stringify(JSON.parse(schemaText))})
      .then((result)=>{
        if(result.compatible){
          return SchemaREST.postVersion(this.props.schemaObj.schemaName, {body: JSON.stringify(data)}, schemaObj.branch.schemaBranch.name);
        } else {
          return result;
        }
      });
  }
  validateSchemaCompatibility = () => {
    let {schemaText} = this.state;
    try{
      const schemaTextStr = JSON.stringify(JSON.parse(schemaText));
      this.setState({schemaTextCompatibility: statusCode.Processing});
      SchemaREST.getCompatibility(this.props.schemaObj.schemaName, {body: schemaTextStr})
        .then((result)=>{
          if(result.compatible){
            this.setState({schemaTextCompatibility: statusCode.Success});
          }else{
            this.setState({schemaTextCompatibility: result.errorMessage || result.responseMessage});
          }
        }).catch(Utils.showError);
    }
    catch(err){
      console.log(err);
    }
  }

  render() {
    const jsonoptions = {
      lineNumbers: true,
      mode: "application/json",
      styleActiveLine: true,
      gutters: this.props.schemaObj.type.toLowerCase() == 'avro' ? ["CodeMirror-lint-markers"] : [],
      lint: this.props.schemaObj.type.toLowerCase() == 'avro'
    };
    let {schemaText, showError, changedFields, showCodemirror, schemaTextCompatibility} = this.state;
    return (
      <form>
        <div className="form-group">
          <label>Description <span className="text-danger">*</span></label>
          <div>
            <input name="description" placeholder="Description" onChange={this.handleValueChange.bind(this)} type="text" className={showError && changedFields.indexOf("description") !== -1 && this.state.description.trim() === ''
              ? "form-control invalidInput"
              : "form-control"} value={this.state.description} required={true}/>
          </div>
        </div>
        <div className="form-group version-codemirror-container">
          <label>Schema Text <span className="text-danger">*</span></label>
          {showCodemirror
            ? [<a key="1" className="pull-right clear-link" href="javascript:void(0)" onClick={() => { this.setState({schemaText: '', showCodemirror: false, schemaTextCompatibility: statusCode.Ok}); }}> CLEAR </a>,
              <span key="2" className="pull-right" style={{margin: '-1px 5px 0'}}>|</span>,
              <a key="3" className="pull-right validate-link" href="javascript:void(0)" onClick={this.validateSchemaCompatibility}>
                VALIDATE
              </a>]
            :
            null
          }
          <div onDrop={this.handleOnDrop.bind(this)} onDragOver={(e) => {
            e.preventDefault();
            e.stopPropagation();
            return false;
          }} style={{"width":"100%"}}>
            {showCodemirror
              ?
              <div style={{"width":"100%", position: 'relative'}}>
                {schemaTextCompatibility === statusCode.Processing
                ?
                <div className="loading-img text-center schema-validating">
                  <img src="../ui/styles/img/start-loader.gif" alt="loading" />
                </div>
                :
                schemaTextCompatibility === statusCode.Ok
                  ?
                  ''
                  :
                  <div className={schemaTextCompatibility === statusCode.Success ? "compatibility-alert success" : "compatibility-alert danger"}>
                    {schemaTextCompatibility === statusCode.Success ? <span className="success">Schema is valid</span> : <span className="danger">{schemaTextCompatibility}</span>}
                    <span className="alert-close"><i className="fa fa-times" onClick={() => this.setState({schemaTextCompatibility: statusCode.Ok})}></i></span>
                  </div>
                }

                <ReactCodemirror ref="JSONCodemirror" value={this.state.schemaText} onChange={this.handleJSONChange.bind(this)} options={jsonoptions}/>
              </div>
              :
              <div ref="browseFileContainer" className={"addSchemaBrowseFileContainer"+(showError && this.props.schemaObj.type.toLowerCase() == 'avro' && !Utils.isValidJson(schemaText) ? ' invalidInput' : '')}>
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
      </form>
    );
  }
}
