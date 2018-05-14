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
import BaseContainer from '../../BaseContainer';
import {Link} from 'react-router';
import FSModal from '../../../components/FSModal';
import {
    DropdownButton,
    MenuItem,
    FormGroup,
    InputGroup,
    FormControl,
    Button,
    PanelGroup,
    Panel,
    Modal,
    Pagination,
    OverlayTrigger,
    Popover
} from 'react-bootstrap';
import Utils, {StateMachine} from '../../../utils/Utils';
import ReactCodemirror from 'react-codemirror';
import '../../../utils/Overrides';
import CodeMirror from 'codemirror';
import 'codemirror/mode/javascript/javascript';
import jsonlint from 'jsonlint';
import lint from 'codemirror/addon/lint/lint';
import SchemaInfoForm from './SchemaInfoForm';
import FSReactToastr from '../../../components/FSReactToastr';
import SchemaREST from '../../../rest/SchemaREST';
import NoData from '../../../components/NoData';
import {toastOpt} from '../../../utils/Constants';
import CommonNotification from '../../../utils/CommonNotification';
import SchemaDetail from '../../../components/SchemaDetail';

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

export default class SchemaRegistryContainer extends Component {
  constructor(props) {
    super();
    this.breadcrumbData = {
      title: 'Schema Registry',
      linkArr: [
        {
          title: 'Registry Service'
        }, {
          title: 'Schema Registry'
        }
      ]
    };

    this.state = {
      modalTitle: '',
      schemaData: [],
      slideInput : false,
      filterValue: '',
      fetchLoader: true,
      sorted : {
        key : 'timestamp',
        text : 'Last Updated'
      },
      expandSchema: false,
      activePage: 1,
      pageSize: 10,
      dataFound: true
    };
    this.schemaObj = {};
    this.schemaText = '';

    this.StateMachine = new StateMachine();
  }
  getChildContext() {
    return {SchemaRegistryContainer: this};
  }
  componentDidMount(){
    this.fetchData().then((entities) => {
      if(!entities.length){
        this.setState({dataFound: false});
      }
    });
    this.fetchStateMachine();
  }
  fetchStateMachine(){
    return SchemaREST.getSchemaVersionStateMachine().then((res) => {
      this.StateMachine.setStates(res);
    }).catch(Utils.showError);
  }
  fetchData() {
    let promiseArr = [],
      schemaData = [],
      schemaCount = 0;

    const {filterValue} = this.state;
    const {key} = this.state.sorted;
    const sortBy = (key === 'name') ? key+',a' : key+',d';

    this.setState({loading: true});
    return SchemaREST.searchAggregatedSchemas(sortBy, filterValue).then((schema) => {
      let schemaEntities = [];
      if (schema.responseMessage !== undefined) {
        FSReactToastr.error(
          <CommonNotification flag="error" content={schema.responseMessage}/>, '', toastOpt);
      } else {
        /*schema.entities.map((obj, index) => {
          let {name, schemaGroup, type, description, compatibility, evolve} = obj.schemaMetadata;
          schemaCount++;

          const versionsArr = [];
          let currentVersion = 0;
          if(obj.versions.length){
            obj.versions.forEach((v) => {
              versionsArr.push({
                versionId: v.version,
                description: v.description,
                schemaText: v.schemaText,
                schemaName: name,
                timestamp: v.timestamp,
                stateId: v.stateId,
                id: v.id
              });
            });
            currentVersion = Utils.sortArray(obj.versions.slice(), 'timestamp', false)[0].version;
          }

          schemaData.push({
            id: (index + 1),
            type: type,
            compatibility: compatibility,
            schemaName: name,
            schemaGroup: schemaGroup,
            evolve: evolve,
            collapsed: true,
            versionsArr:  versionsArr,
            currentVersion: currentVersion,
            serDesInfos: obj.serDesInfos
          });
          schemaEntities = schemaData;
        });*/
        schemaEntities = schema.entities;
        let dataFound = this.state.dataFound;
        if(!dataFound && schemaEntities.length){
          dataFound = true;
        }
        this.setState({schemaData: schemaEntities, loading: false, dataFound: dataFound});
        if(schema.entities.length === 0) {
          this.setState({loading: false});
        }
      }
      return schemaEntities;
    }).catch(Utils.showError);
  }
  slideInput = (e) => {
    this.setState({slideInput  : true});
    const input = document.querySelector('.inputAnimateIn');
    input.focus();
  }
  slideInputOut = () =>{
    const input = document.querySelector('.inputAnimateIn');
    (_.isEmpty(input.value)) ? this.setState({slideInput  : false}) : '';
  }
  onFilterChange = (e) => {
    this.setState({filterValue: e.target.value});
  }
  onFilterKeyPress = (e) => {
    if(e.key=='Enter'){
      this.setState({filterValue: e.target.value.trim(), activePage: 1}, () => {
        this.fetchData();
      });
    }
  }
  filterSchema(entities, filterValue){
    let matchFilter = new RegExp(filterValue , 'i');
    return entities.filter(e => !filterValue || matchFilter.test(e.schemaName));
  }
  onSortByClicked = (eventKey,el) => {
    const liList = el.target.parentElement.parentElement.children;
    let {schemaData} = this.state;
    for(let i = 0;i < liList.length ; i++){
      liList[i].setAttribute('class','');
    }
    el.target.parentElement.setAttribute("class","active");
    //if sorting by name, then in ascending order
    //if sorting by timestamp, then in descending order

    const sortObj = {key : eventKey , text : this.sortByKey(eventKey)};
    this.setState({sorted : sortObj}, () => {
      this.fetchData();
    });
  }
  sortByKey = (string) => {
    switch (string) {
    case "timestamp": return "Last Updated";
      break;
    case "name" : return "Name";
      break;
    default: return "Last Updated";
    }
  }
  handleAddSchema() {
    this.setState({
      modalTitle: 'Add New Schema'
    }, () => {
      this.refs.schemaModal.show();
    });
  }
  handleSave() {
    if (this.refs.addSchema.validateData()) {
      this.refs.addSchema.handleSave(this.versionFailed).then((schemas) => {
        if (schemas.responseMessage !== undefined) {
          this.versionFailed = true;
          FSReactToastr.error(<CommonNotification flag="error" content={schemas.responseMessage}/>, '', toastOpt);
        } else {
          this.versionFailed = false;
          this.refs.schemaModal.hide();
          this.fetchData();
          let msg = "Schema added successfully";
          if (this.state.id) {
            msg = "Schema updated successfully";
          }
          FSReactToastr.success(
            <strong>{msg}</strong>
          );
        }
      }).catch(Utils.showError);
    }
  }

  handlePagination = (eventKey) => {
    this.setState({
      activePage: eventKey
    });
  }

  getActivePageData = (allData, activePage, pageSize) => {
    activePage = activePage - 1;
    const startIndex = activePage*pageSize;
    return allData.slice(startIndex, startIndex+pageSize);
  }

  render() {
    const schemaViewOptions = {
      lineNumbers: true,
      mode: "application/json",
      styleActiveLine: true,
      gutters: ["CodeMirror-lint-markers"],
      lint: false,
      readOnly: true,
      theme: 'default no-cursor schema-editor expand-schema'
    };
    const {slideInput, schemaData, activePage, pageSize} = this.state;
    const sortTitle = <span>Sort:<span className="font-blue-color">&nbsp;{this.state.sorted.text}</span></span>;
    var schemaEntities = schemaData;
    /*if(filterValue.trim() !== ''){
      schemaEntities = this.filterSchema(schemaData, filterValue);
    }*/
    const activePageData = this.getActivePageData(schemaEntities, activePage, pageSize);

    const panelContent = activePageData.map((s, i)=>{
      return <SchemaDetail schema={s} key={i} StateMachine={this.StateMachine}/>;
    });


    return (
      <BaseContainer routes={this.props.routes} onLandingPage="false" breadcrumbData={this.breadcrumbData} headerContent={'All Schemas'}>
          <div id="add-schema">
              <button role="button" type="button" className="actionAddSchema hb lg success" onClick={this.handleAddSchema.bind(this)}>
                  <i className="fa fa-plus"></i>
              </button>
          </div>
          {!this.state.loading && this.state.dataFound ?
            <div className="wrapper animated fadeIn">
              <div className="page-title-box row no-margin">
                  <div className="col-md-3 col-md-offset-6 text-right">
                    <FormGroup>
                       <InputGroup>
                         <FormControl type="text" placeholder="Search by name" value={this.state.filterValue} onChange={this.onFilterChange} onKeyPress={this.onFilterKeyPress} className="" />
                           <InputGroup.Addon>
                             <i className="fa fa-search"></i>
                           </InputGroup.Addon>
                         </InputGroup>
                     </FormGroup>
                  </div>
                  <div className="col-md-2 text-center">
                    <DropdownButton title={sortTitle}
                      id="sortDropdown"
                      className="sortDropdown"
                    >
                        <MenuItem active={this.state.sorted.key == 'name' ? true : false} onClick={this.onSortByClicked.bind(this,"name")}>
                            &nbsp;Name
                        </MenuItem>
                        <MenuItem active={this.state.sorted.key == 'timestamp' ? true : false} onClick={this.onSortByClicked.bind(this,"timestamp")}>
                            &nbsp;Last Update
                        </MenuItem>
                    </DropdownButton>
                  </div>
              </div>

              {!this.state.loading && schemaEntities.length ?
                <div className="row">
                    <div className="col-md-12">
                      <PanelGroup
                          bsClass="panel-registry"
                          role="tablist"
                      >
                        {panelContent}
                      </PanelGroup>
                      {schemaEntities.length > pageSize
                        ?
                        <div className="text-center">
                          <Pagination
                            prev
                            next
                            first
                            last
                            ellipsis
                            boundaryLinks
                            items={Math.ceil(schemaEntities.length/pageSize)}
                            maxButtons={5}
                            activePage={this.state.activePage}
                            onSelect={this.handlePagination} />
                        </div>
                        : null
                       }
                    </div>
                </div>
                : !this.state.loading ? <NoData /> : null}
            </div>
            : !this.state.loading ? <NoData /> : null}

          {this.state.loading ?
            <div className="col-sm-12">
              <div className="loading-img text-center" style={{marginTop : "50px"}}>
                <img src="../ui/styles/img/start-loader.gif" alt="loading" />
              </div>
            </div>
            : null}

        <FSModal ref="schemaModal" bsSize="large" data-title={this.state.modalTitle} data-resolve={this.handleSave.bind(this)}>
          <SchemaInfoForm ref="addSchema"/>
        </FSModal>
        <Modal dialogClassName="modal-xl" ref="expandSchemaModal" bsSize="large" show={this.state.expandSchema} onHide={()=>{this.setState({ expandSchema: false });}}>
          <Modal.Header closeButton>
            <Modal.Title>{this.state.modalTitle}</Modal.Title>
          </Modal.Header>
          <Modal.Body>
            {this.state.expandSchema ?
            <ReactCodemirror
              ref="JSONCodemirror"
              value={JSON.stringify(JSON.parse(this.schemaText), null, ' ')}
              options={schemaViewOptions}
            /> : ''}
          </Modal.Body>
          <Modal.Footer>
            <Button onClick={()=>{this.setState({ expandSchema: false });}}>Close</Button>
          </Modal.Footer>
        </Modal>
      </BaseContainer>

    );
  }
}

SchemaRegistryContainer.childContextTypes = {
  SchemaRegistryContainer: PropTypes.object
};
