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
import SchemaREST from '../rest/SchemaREST';
import FSReactToastr from '../components/FSReactToastr';
import Utils from '../utils/Utils';

export default class ChangeState extends Component{
  constructor(props){
    super(props);
    this.state = {
      edit: false
    };
  }

  changeState(e){
    const {version} = this.props;
    if(version.stateId === parseInt(this.refs.stateSelect.value, 10)) {
      this.setState({edit: false});
      return;
    }
    SchemaREST.changeStateOfVersion(version.id, this.refs.stateSelect.value, {}).then((res) => {
      version.stateId = parseInt(this.refs.stateSelect.value);
      this.setState({edit: false});
      this.props.stateChangeCallback();
    }).catch(Utils.showError);
  }
  onEdit(){
    this.setState({edit: true}, () => {});
  }
  render(){
    const {edit} = this.state;
    const {StateMachine, version, showEditBtn} = this.props;
    const transitions = StateMachine.getTransitionStateOptions(version.stateId);
    const currentState = StateMachine.getStateById(version.stateId).name;
    let comp;
    if(edit){
      comp = <div style={{"marginTop": "5px", "display": "inline"}}>
        <select ref="stateSelect" className="stateSelect" defaultValue={version.stateId}>
          <option disabled value={version.stateId}>{currentState}</option>
          {transitions.map( option =>
            (<option value={option.targetStateId} key={option.targetStateId}>{option.name}</option>)
          )}
        </select>
        &nbsp;
        <a href="javascript:void(0)" className="btn-stateSelect" onClick={this.changeState.bind(this)}>
          <i className="fa fa-check" aria-hidden="true"></i>
        </a>
        &nbsp;
        <a href="javascript:void(0)" className="btn-stateSelect" onClick={() => this.setState({edit: false})}>
          <i className="fa fa-times" aria-hidden="true"></i>
        </a>
      </div>;
    }else{
      comp = <div style={{"display": "inline"}}>
          <span className="text-muted">{currentState}</span>
          &nbsp;
          {transitions.length &&  showEditBtn?
          <a href="javascript:void(0)" onClick={this.onEdit.bind(this)}><i className="fa fa-pencil" aria-hidden="true"></i></a>
          : ''
          }
        </div>;
    }
    return comp;
  }
}