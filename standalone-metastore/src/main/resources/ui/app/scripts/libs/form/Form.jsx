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
import _ from 'lodash';
import {
  Button,
  Form,
  FormGroup,
  Col,
  FormControl,
  Checkbox,
  Radio,
  ControlLabel
} from 'react-bootstrap';

export default class FSForm extends Component {
  /*state = {
        FormData: {},
        Errors: {}
    }*/
  constructor(props) {
    super(props);
    this.state = {
      FormData: props.FormData,
      Errors: props.Errors
    };
  }
  componentDidMount = () => {
    this.setState({Errors : {}});
  }
  componentWillReceiveProps = (nextProps) => {
    if (this.props.FormData != nextProps.FormData) {
      this.updateFormData(nextProps.FormData);
    }
  }
  updateFormData(newFormData) {

    for (let key in this.state.Errors) {
      delete this.state.Errors[key];
    }

    this.setState({
      FormData: _.assignInWith(this.state.FormData, _.cloneDeep(newFormData)),
      Errors: this.state.Errors
    });
  }
  getChildContext() {
    return {Form: this};
  }
  render() {
    return (
      <Form className={this.props.className} style={this.props.style}>
        {this.props.children.map((child, i) => {
          const {fieldJson = {}} = child.props;
          return React.cloneElement(child, {
            ref: child.props
              ? (child.props._ref || i)
              : i,
            key: i,
            data: this.state.FormData,
            className: this.props.showRequired == null
              ? ''
              : this.props.showRequired
                ? !fieldJson.isOptional
                  ? ''
                  : 'hidden' : fieldJson.isOptional
                    ? ''
                    : 'hidden'
          });
        })}
      </Form>
    );
  }
  validate() {
    let isFormValid = true;
    for (let key in this.refs) {
      let component = this.refs[key];
      if (component.type == "FormField") {
        let isFieldValid = false;
        isFieldValid = component.validate();

        if (isFormValid) {
          isFormValid = isFieldValid;
        }
      }
    }
    return isFormValid;
  }
}

FSForm.defaultProps = {
  showRequired: true,
  readOnly: false,
  Errors: {},
  FormData: {}
};

FSForm.childContextTypes = {
  Form: PropTypes.object
};
