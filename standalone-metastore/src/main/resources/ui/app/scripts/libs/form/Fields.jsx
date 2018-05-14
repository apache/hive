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
  Button,
  Form,
  FormGroup,
  Col,
  FormControl,
  Checkbox,
  Radio,
  ControlLabel,
  Popover,
  InputGroup,
  OverlayTrigger
} from 'react-bootstrap';
import Select, {Creatable} from 'react-select';
import validation from './ValidationRules';
import _ from 'lodash';
import Utils from '../../utils/Utils';

export class BaseField extends Component {
  type = 'FormField';
  getField = () => {}
  validate(value) {
    let errorMsg = '';
    if (this.props.validation) {
      this.props.validation.forEach((v) => {
        if (errorMsg == '') {
          errorMsg = validation[v](value, this.context.Form, this);
        } else {
          return;
        }
      });
    }
    const {Form} = this.context;
    Form.state.Errors[this.props.valuePath] = errorMsg;
    Form.setState(Form.state);
    return !errorMsg;
  }

  render() {
    const {className} = this.props;
    return (
      <FormGroup className={className}>
        <label>{this.props.label}
        {this.props.validation && this.props.validation.indexOf('required') !== -1
          ?
          <span className="text-danger">*</span>
          :
          null
        }
        </label>
        {this.getField()}
        {<p className="text-danger">{this.context.Form.state.Errors[this.props.valuePath]}</p>}
      </FormGroup>
    );
  }
}

BaseField.contextTypes = {
  Form: PropTypes.object
};


export class String extends BaseField {
  handleChange = () => {
    const value = this.refs.input.value;
    const {Form} = this.context;
    this.props.data[this.props.value] = value;
    Form.setState(Form.state, () => {
      this.validate(value);
    });
  }

  validate(){
    return super.validate(this.props.data[this.props.value]);
  }

  getField = () => {
    let disabledField = this.context.Form.props.readOnly;

    return (<input type="text" className={
        this.context.Form.state.Errors[this.props.valuePath]
        ?
        "form-control invalidInput"
        :
        "form-control"
      }
      ref="input"
      value={this.props.data[this.props.value] || ''}
      disabled={disabledField}
      {...this.props.attrs}
      onChange={this.handleChange}/>
    );
  }
}

export class TextArea extends String {
  getField = () => {
    let disabledField = this.context.Form.props.readOnly;

    return (<textarea className={
        this.context.Form.state.Errors[this.props.valuePath]
        ?
        "form-control invalidInput"
        :
        "form-control"
      }
      ref="input"
      value={this.props.data[this.props.value] || ''}
      disabled={disabledField}
      {...this.props.attrs}
      onChange={this.handleChange}
      style={{maxWidth: '100%'}}
      />
    );
  }
}


export class File extends BaseField {
  handleChange = (e) => {
    const value = e.target.files[0];
    const {Form} = this.context;
    this.props.data[this.props.value] = value;
    Form.setState(Form.state, () => {
      this.validate(value);
    });
  }

  validate(){
    return super.validate(this.props.data[this.props.value]);
  }

  getField = () => {
    let disabledField = this.context.Form.props.readOnly;

    return (<input type="file" className={
        this.context.Form.state.Errors[this.props.valuePath]
        ?
        "form-control invalidInput"
        :
        "form-control"
      }
      ref="input"
      disabled={disabledField}
      {...this.props.attrs}
      onChange={this.handleChange}/>
    );
  }
}