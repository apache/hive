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
    Tooltip,
    Popover
} from 'react-bootstrap';
import FSModal, {Confirm} from './FSModal';
import {toastOpt} from '../utils/Constants';
import FSReactToastr from './FSReactToastr';
import CommonNotification from '../utils/CommonNotification';
import Utils, {StateMachine} from '../utils/Utils';
import {Link} from 'react-router';
import ReactCodemirror from 'react-codemirror';
import Select from 'react-select';
import SchemaREST from '../rest/SchemaREST';
import SchemaVersionDiff from '../containers/Registry-Services/SchemaRegistry/SchemaVersionDiff';
import SchemaVersionForm from '../containers/Registry-Services/SchemaRegistry/SchemaVersionForm';
import ChangeState from './SchemaChangeState';
import ForkBranch from './ForkBranchForm';
import SchemaBranches from './SchemaBranches';

export default class SchemaDetail extends Component{
  constructor(props){
    super(props);
    const {schema} = props;
    const selectedBranch = _.find(schema.schemaBranches, (branch) => {
      return branch.schemaBranch.name == 'MASTER';
    });
    const currentVersion = Utils.sortArray(selectedBranch.schemaVersionInfos.slice(), 'timestamp', false)[0].version;
    this.state = {
      collapsed: true,
      renderCodemirror: false,
      selectedBranch: selectedBranch,
      currentVersion: currentVersion
    };
    // this.fetchBranches();
  }
  fetchBranches(){
    const {schema} = this.props;
    const {name} = schema.schemaMetadata;
    SchemaREST.getBranches(name, {}).then((res)=>{
      const selectedBranch = _.find(res.entities, (b) => {
        return b.name == "MASTER";
      });
      this.setState({branches: res.entities, selectedBranch});
    }).catch(Utils.showError);
  }
  getAggregatedSchema(){
    const {schema} = this.props;
    const {name} = schema.schemaMetadata;
    return SchemaREST.getAggregatedSchema(name).then((res) => {
      _.each(schema, (val, key) => {
        schema[key] = res[key];
      });
      this.forceUpdate();
    }).catch(Utils.showError);
  }
  componentDidMount(){
    this.btnClassChange();
  }
  componentDidUpdate(){
    this.btnClassChange();
  }
  btnClassChange = () => {
    const {schemaData, loading} = this.context.SchemaRegistryContainer.state;
    if(!loading){
      if(schemaData.length !== 0){
        const sortDropdown = document.querySelector('.sortDropdown');
        sortDropdown.setAttribute("class","sortDropdown");
        sortDropdown.parentElement.setAttribute("class","dropdown");
        if(this.refs.schemaName && this.schemaNameTagWidth != this.refs.schemaName.offsetWidth){
          this.schemaNameTagWidth = this.refs.schemaName.offsetWidth;
          this.schemaGroupTagWidth= this.refs.schemaGroup.offsetWidth;
          this.setState(this.state);
        }
      }
    }
  }
  getBtnClass(c) {
    switch(c){
    case 'FORWARD':
      return "warning";
    case 'BACKWARD':
      return "backward";
    case 'BOTH':
      return "";
    default:
      return 'default';
    }
  }
  getIconClass(c) {
    switch(c){
    case 'FORWARD':
      return "fa fa-arrow-right";
    case 'BACKWARD':
      return "fa fa-arrow-left";
    case 'BOTH':
      return "fa fa-exchange";
    case 'NONE':
      return "fa fa-ban";
    default:
      return '';
    }
  }
  handleSelect(s, k, e){
    /*const {SchemaRegistryContainer} = this.context;
    let {schemaData} = SchemaRegistryContainer.state;
    let schema = _.find(schemaData,{id: s.id});
    let obj = {};
    schema.collapsed = !s.collapsed;
    obj.schemaData = schemaData;
    SchemaRegistryContainer.setState(obj);*/
    const {collapsed} = this.state;
    this.setState({collapsed : !collapsed});
  }
  handleOnEnter(s){
    this.setState({renderCodemirror: true});
  }
  handleOnExit(s){
    this.setState({renderCodemirror: false});
  }
  selectVersion(v) {
    // let {schemaData} = this.state;
    // let obj = _.find(schemaData, {schemaName: v.schemaName});
    this.setState({currentVersion: v.version});
  }
  handleAddVersion (schemaObj) {
    const {selectedBranch, currentVersion} = this.state;
    let obj = _.find(selectedBranch.schemaVersionInfos, {version: currentVersion});
    this.schemaObj = {
      schemaName: schemaObj.schemaMetadata.name,
      description: obj ? obj.description : '',
      schemaText: obj ? obj.schemaText : '',
      versionId: obj ? obj.version : '',
      branch: selectedBranch,
      type: this.props.schema.schemaMetadata.type
    };
    this.setState({
      modalTitle: 'Edit Version'
    }, () => {
      this.refs.versionModal.show();
    });
  }
  handleSaveVersion() {
    if (this.refs.addVersion.validateData()) {
      this.refs.addVersion.handleSave().then((versions) => {
        if(versions && versions.compatible === false){
          FSReactToastr.error(<CommonNotification flag="error" content={versions.errorMessage}/>, '', toastOpt);
        } else {
          if (versions.responseMessage !== undefined) {
            FSReactToastr.error(
              <CommonNotification flag="error" content={versions.responseMessage}/>, '', toastOpt);
          } else {
            this.refs.versionModal.hide();
            /*const {SchemaRegistryContainer} = this.context;
            SchemaRegistryContainer.fetchData();*/
            const {selectedBranch} = this.state;
            this.fetchAndSelectBranch(selectedBranch.schemaBranch.name);
            let msg = "Version added successfully";
            if (this.state.modalTitle === 'Edit Version') {
              msg = "Version updated successfully";
            }
            if(versions === this.schemaObj.version) {
              msg = "The schema version is already present";
              FSReactToastr.info(
                <strong>{msg}</strong>
              );
            } else {
              FSReactToastr.success(
                <strong>{msg}</strong>
              );
            }
          }
        }
      }).catch(Utils.showError);
    }
  }
  handleCompareVersions(schemaObj) {
    // this.schemaObj = schemaObj;
    this.setState({
      modalTitle: 'Compare Schema Versions',
      showDiffModal: true
    });
  }
  handleExpandView(schemaObj) {
    let obj = _.find(schemaObj.versions, {version: schemaObj.currentVersion});
    this.schemaText = obj.schemaText;
    this.setState({
      modalTitle: obj.schemaName,
      expandSchema: true
    }, () => {
      this.setState({ expandSchema: true});
    });
  }
  onBranchSelect = (val) => {
    const currentVersion = Utils.sortArray(val.schemaVersionInfos.slice(), 'timestamp', false)[0].version;
    this.setState({selectedBranch: val, currentVersion});
  }
  onFork(v){
    this.refs.ForkBranch.show();
  }
  fetchAndSelectBranch = (branchName, version) => {
    branchName = branchName || 'MASTER';
    this.getAggregatedSchema().then(() => {
      const newBranch = _.find(this.props.schema.schemaBranches, (branch) => {
        return branch.schemaBranch.name == branchName;
      });
      const currentVersion = version || Utils.sortArray(newBranch.schemaVersionInfos.slice(), 'timestamp', false)[0].version;
      this.setState({selectedBranch: newBranch, currentVersion});
    }).catch(Utils.showError);
  }
  onMerge(v){
    const {schema} = this.props;
    this.refs.Confirm.show({title: 'Are you sure you want to merge this branch?'}).then((confirmBox) => {
      SchemaREST.mergeBranch(v.id, {}).then((res) => {
        if (res.responseMessage !== undefined) {
          FSReactToastr.error(<CommonNotification flag="error" content={res.responseMessage}/>, '', toastOpt);
        }else{
          FSReactToastr.success(<strong>{res.mergeMessage}</strong>);
          let branchName = 'MASTER';
          schema.schemaBranches.forEach((b)=>{
            const hasVersion = b.schemaVersionInfos.find((v)=>{
              return v.version == res.schemaIdVersion.version && v.id == res.schemaIdVersion.schemaVersionId;
            });

            if(hasVersion) {
              branchName = b.schemaBranch.name;
            }
          });
          this.fetchAndSelectBranch(branchName, res.schemaIdVersion.version);
        }
      }).catch(Utils.showError);
      confirmBox.cancel();
    });
  }
  onDeleteBranch = () => {
    const {selectedBranch} = this.state;
    const branchId = selectedBranch.schemaBranch.id;
    this.refs.Confirm.show({title: 'Are you sure you want to delete this branch?'}).then((confirmBox) => {
      SchemaREST.deleteBranch(branchId, {}).then((res) => {
        if (res.responseMessage !== undefined) {
          FSReactToastr.error(<CommonNotification flag="error" content={res.responseMessage}/>, '', toastOpt);
        } else {
          FSReactToastr.success(
            <strong>Branch Deleted Successfully</strong>
          );
          this.fetchAndSelectBranch();
        }
      }).catch(Utils.showError);
      confirmBox.cancel();
    });
  }
  handleSaveFork(){
    const {currentVersion} = this.state;
    const version = _.find(this.state.selectedBranch.schemaVersionInfos, {version: currentVersion});
    const Form = this.refs.ForkBranchForm.refs.Form;
    const {FormData} = Form.state;
    if(Form.validate()){
      SchemaREST.forkNewBranch(version.id, {
        body: JSON.stringify(FormData)
      }).then((res) => {
        if (res.responseMessage !== undefined) {
          FSReactToastr.error(<CommonNotification flag="error" content={res.responseMessage}/>, '', toastOpt);
        }else{
          FSReactToastr.success(<strong>Branch Created Successfully</strong>);
          this.refs.ForkBranch.hide();
          this.fetchAndSelectBranch(FormData.name);
        }
      }).catch(Utils.showError);;
    }
  }
  getHeader(){
    const {schema} = this.props;
    const {collapsed} = this.state;
    const s = schema;

    const {compatibility, type, schemaGroup, name} = s.schemaMetadata;

    var btnClass = this.getBtnClass(compatibility);
    var iconClass = this.getIconClass(compatibility);
    // var totalVersions = s.versions.length;
    
    var header = (
      <div>
        <span className={`hb ${btnClass} schema-status-icon`}><i className={iconClass}></i></span>
        <div className="panel-sections first fluid-width-15">
          <h4 ref="schemaName" className="schema-name" title={name}>{Utils.ellipses(name, this.schemaNameTagWidth)}</h4>
          <p className={`schema-status ${compatibility.toLowerCase()}`}>{compatibility}</p>
        </div>
        <div className="panel-sections">
          <h6 className="schema-th">Type</h6>
          <h4 className={`schema-td ${!collapsed ? "font-blue-color" : ''}`}>{type}</h4>
        </div>
        <div className="panel-sections">
          <h6 className="schema-th">Group</h6>
          <h4 ref="schemaGroup" className={`schema-td ${!collapsed ? "font-blue-color" : ''}`} title={schemaGroup}>{Utils.ellipses(schemaGroup, this.schemaGroupTagWidth)}</h4>
        </div>
        <div className="panel-sections">
          <h6 className="schema-th">Branch</h6>
          <h4 className={`schema-td ${!collapsed ? "font-blue-color" : ''}`}>{s.schemaBranches.length}&nbsp;&nbsp;<a title="View Branches" style={{display: 'inline', cursor: 'pointer'}} onClick={this.showSchemaBranches}><i className="fa fa-code-fork fa-rotate-90"></i></a></h4>
        </div>
        <div className="panel-sections">
          <h6 className="schema-th">Serializer & Deserializer</h6>
          <h4 className={`schema-td ${!collapsed ? "font-blue-color" : ''}`}><Link to={"/schemas/"+name+'/serdes'} style={{display:'inline'}}>{s.serDesInfos.length}</Link></h4>
        </div>
        <div className="panel-sections" style={{'textAlign': 'right'}}>
          <a className="collapsed collapseBtn" role="button" aria-expanded="false">
            <i className={collapsed ? "collapseBtn fa fa-chevron-down" : "collapseBtn fa fa-chevron-up"}></i>
          </a>
        </div>
      </div>
    );
    return header;
  }
  branchOptionRenderer = (opt) => {
    return opt.schemaBranch.name;
  }
  branchValueRenderer = (val) => {
    return val.schemaBranch.name;
  }
  stateChangeCallback = () =>{
    let branchName = this.state.selectedBranch.schemaBranch.name;
    const {schema} = this.props;
    this.getAggregatedSchema().then((res)=>{
      const currentBranch = _.find(schema.schemaBranches, (branch) => {
        return branch.schemaBranch.name == branchName;
      });
      this.setState({selectedBranch: currentBranch});
    });
  }
  showSchemaBranches = (e) => {
    this.setState({
      modalTitle: 'Schema Branches',
      showBranchesModal: true
    });
    e.stopPropagation();
  }
  render(){
    const {schema, key, StateMachine} = this.props;
    const {selectedBranch, collapsed, renderCodemirror, currentVersion} = this.state;
    const s = schema;
    const {name, evolve} = s.schemaMetadata;
    const currentBranchName = selectedBranch.schemaBranch.name;
    const enabledStateId = StateMachine.getStateByName('Enabled').id;

    const jsonoptions = {
      lineNumbers: true,
      mode: "application/json",
      styleActiveLine: true,
      gutters: ["CodeMirror-lint-markers"],
      lint: false,
      readOnly: true,
      theme: 'default no-cursor schema-editor'
    };

    var versionObj = _.find(selectedBranch.schemaVersionInfos, {version: currentVersion});
    var sortedVersions =  Utils.sortArray(selectedBranch.schemaVersionInfos.slice(), 'version', false);

    const expandButton = ' ' || <button key="e.3" type="button" className="btn btn-link btn-expand-schema" onClick={this.handleExpandView.bind(this, s)}>
      <i className="fa fa-arrows-alt"></i>
    </button>;

    return (
      <Panel
        header={this.getHeader()}
        headerRole="tabpanel"
        key={name}
        collapsible
        expanded={collapsed ? false : true}
        onSelect={this.handleSelect.bind(this, s)}
        onEntered={this.handleOnEnter.bind(this, s)}
        onExited={this.handleOnExit.bind(this, s)}
      >
        {collapsed ?
        '': (versionObj ? (
        <div className="panel-registry-body">
          <div className="row">
            <div className="col-sm-3">
              <div className="row">
                <h6 className="schema-th"><strong>Branch :</strong></h6>
                <Select
                  value={selectedBranch}
                  options={schema.schemaBranches}
                  onChange={this.onBranchSelect}
                  optionRenderer={this.branchOptionRenderer}
                  valueRenderer={this.branchValueRenderer}
                  clearable={false}
                />
              </div>
              <br />
              <div className="row">
                <h6 className="schema-th"><strong>Branch Description :</strong></h6>
                <p>{selectedBranch.schemaBranch.description}</p>
              </div>
              <div className="row">
                <h6 className="schema-th"><strong>Version Description :</strong></h6>
                <p>{versionObj.description}</p>
              </div>
              {currentBranchName == 'MASTER' && versionObj.mergeInfo !== null?
                <div className="row">
                <h6 className="schema-th"><strong>Merge Detail :</strong></h6>
                <p>{'Merged from version ' + versionObj.mergeInfo.schemaVersionId + ' of branch "' + versionObj.mergeInfo.schemaBranchName + '".'}</p>
                </div>
              : ''}
            </div>
            <div className="col-sm-6">
              {renderCodemirror ?
              (evolve ? ([
                <h6 key="e.1" className="version-number-text">VERSION&nbsp;{versionObj.version}</h6>,
                <button key="e.2" type="button" className="btn btn-link btn-edit-schema" onClick={this.handleAddVersion.bind(this, s)}>
                  <i className="fa fa-pencil"></i>
                </button>,
                expandButton]) : expandButton)
              : ''}
              {renderCodemirror ?
              (<ReactCodemirror
                ref="JSONCodemirror"
                value={JSON.stringify(JSON.parse(versionObj.schemaText), null, ' ')}
                options={jsonoptions}
              />)
              : (
              <div className="col-sm-12">
                <div className="loading-img text-center" style={{marginTop : "50px"}}>
                  <img src="../ui/styles/img/start-loader.gif" alt="loading" />
                </div>
              </div>)}
            </div>
            <div className="col-sm-3">
              <h6 className="schema-th" style={{textTransform: 'none'}}><strong>BRANCH: {currentBranchName}</strong> {currentBranchName !== 'MASTER' ? <a onClick={this.onDeleteBranch} className="text-danger" style={{cursor: 'pointer'}}><i className="fa fa-trash"></i></a> : ''}</h6>
              <h6 className="schema-th">Change Log</h6>
              <ul className="version-tree">
                {
                sortedVersions.map((v, i)=>{
                  const forkMergeComp = [<OverlayTrigger placement="top" overlay={<Tooltip id="fork">Fork</Tooltip>}>
                    <a href="javascript:void(0)" onClick={this.onFork.bind(this, v)} style={{marginLeft: '5px'}}>
                      <i className="fa fa-code-fork" aria-hidden="true"></i>
                    </a></OverlayTrigger>,
                    currentBranchName !== 'MASTER' ?
                      <OverlayTrigger placement="top" overlay={<Tooltip id="merge">Merge</Tooltip>}>
                      <a href="javascript:void(0)" onClick={this.onMerge.bind(this, v)} style={{marginLeft: '5px'}}>
                        <i className="fa fa-code-fork fa-rotate-180" aria-hidden="true"></i>
                      </a></OverlayTrigger>
                      : ''];
                  return (
                    <li onClick={this.selectVersion.bind(this, v)} key={i} className={currentVersion === v.version? "clearfix current" : "clearfix"}>
                      <a className={currentVersion === v.version? "hb version-number" : "hb default version-number"}>v{v.version}</a>
                      <p>
                        <span className="log-time-text">{Utils.splitTimeStamp(new Date(v.timestamp))}</span>
                        <br/>
                      </p>
                      <ChangeState
                        version={v}
                        StateMachine={StateMachine}
                        showEditBtn={currentVersion === v.version && !(currentBranchName !== 'MASTER' && i == sortedVersions.length-1)}
                        stateChangeCallback={this.stateChangeCallback}
                      />
                      {currentVersion === v.version && !(currentBranchName !== 'MASTER' && i == sortedVersions.length-1) && v.stateId == enabledStateId ?
                      forkMergeComp
                      : ''}
                  </li>
                  );
                })
                }
              </ul>
              {sortedVersions.length > 1 ?
                <a className="compare-version" onClick={this.handleCompareVersions.bind(this, s)}>COMPARE VERSIONS</a>
                : ''}
            </div>
          </div>
        </div>) :
        (<div className="panel-registry-body">
          <div className="row">
          {evolve ? ([
            <div className="col-sm-3" key="v.k.1">
              <h6 className="schema-th">Description</h6>
              <p></p>
            </div>,
            <div className="col-sm-6" key="v.k.2">
              {renderCodemirror ?
              <button type="button" className="btn btn-link btn-add-schema" onClick={this.handleAddVersion.bind(this, s)}>
                <i className="fa fa-pencil"></i>
              </button>
              : ''
              }
              {renderCodemirror ?
              (<ReactCodemirror
                ref="JSONCodemirror"
                value=""
                options={jsonoptions}
              />)
              : (
              <div className="col-sm-12">
                <div className="loading-img text-center" style={{marginTop : "50px"}}>
                  <img src="../ui/styles/img/start-loader.gif" alt="loading" />
                </div>
              </div>)
              }
            </div>,
            <div className="col-sm-3" key="v.k.3">
              <h6 className="schema-th">Change Log</h6>
            </div>])
            : <div style={{'textAlign': 'center'}}>NO DATA FOUND</div>
            }
          </div>
        </div>)
      )}
        <FSModal ref="versionModal" data-title={this.state.modalTitle} data-resolve={this.handleSaveVersion.bind(this)}>
          <SchemaVersionForm ref="addVersion" schemaObj={this.schemaObj}/>
        </FSModal>
        <Modal ref="schemaDiffModal" bsSize="large" show={this.state.showDiffModal} onHide={()=>{this.setState({ showDiffModal: false });}}>
          <Modal.Header closeButton>
            <Modal.Title>{this.state.modalTitle}</Modal.Title>
          </Modal.Header>
          <Modal.Body>
            <SchemaVersionDiff ref="compareVersion" schemaObj={selectedBranch}/>
          </Modal.Body>
          <Modal.Footer>
            <Button onClick={()=>{this.setState({ showDiffModal: false });}}>Close</Button>
          </Modal.Footer>
        </Modal>
        <FSModal ref="ForkBranch" data-title={"Fork a New Schema Branch"} data-resolve={this.handleSaveFork.bind(this)}>
          <ForkBranch ref="ForkBranchForm" FormData={{}}/>
        </FSModal>
        <Modal ref="schemaBranchesModal" bsSize="large" show={this.state.showBranchesModal} onHide={()=>{this.setState({ showBranchesModal: false });}}>
          <Modal.Header closeButton>
            <Modal.Title>{this.state.modalTitle}</Modal.Title>
          </Modal.Header>
          <Modal.Body>
            <SchemaBranches schema={this.props.schema}/>
          </Modal.Body>
          <Modal.Footer>
            <Button onClick={()=>{this.setState({ showBranchesModal: false });}}>Close</Button>
          </Modal.Footer>
        </Modal>
        <Confirm ref="Confirm"/>
      </Panel>
    );
  }
}

SchemaDetail.contextTypes = {
  SchemaRegistryContainer: PropTypes.object
};