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
import {Link} from 'react-router';
import BaseContainer from '../../BaseContainer';
import { Table, Thead, Th, Tr, Td, unsafe } from 'reactable';

import FSReactToastr from '../../../components/FSReactToastr';
import Utils from '../../../utils/Utils';

import SerializerDeserializerForm from './SerializerDeserializerForm';
import FSModal from '../../../components/FSModal';

import FileREST from '../../../rest/FileREST';
import SerializerDeserializerREST from '../../../rest/SerializerDeserializerREST';

export default class SerializerDeserializer extends Component {
  constructor(props) {
    super(props);
    this.state = {
      tableData: [],
      FormData: {}
    };
    this.fetchData();
  }

  fetchData(){
    const {schemaName} = this.props.routeParams;
    SerializerDeserializerREST.getSerDess(schemaName, {}).then((res) => {
      this.setState({
        tableData: res.entities || []
      });
    });
  }

  handleAdd = () => {
    this.refs.addModal.show();
  }

  addNew = () => {
    const {addSerializerDeserializerForm} = this.refs;
    const {Form} = addSerializerDeserializerForm.refs;

    if(!Form.validate()){
      return;
    }

    const {schemaName} = this.props.routeParams;
    const data = Form.state.FormData;
    const formData = new FormData();
    formData.append('file', data.file);
    FileREST.postFile({body: formData})
      .then((fileId) => {
        delete data.file;
        data.fileId = fileId;
        SerializerDeserializerREST.postSerDes({body: JSON.stringify(data)})
          .then((id) => {
            SerializerDeserializerREST.postMapping(schemaName, id)
              .then((res) => {
                FSReactToastr.success(
                  <strong>Serializer & Deserializer added successfully</strong>
                );
                this.fetchData();
                this.refs.addModal.hide();
                this.clearFormData();
              })
              .catch(Utils.showError);
          })
          .catch(Utils.showError);
      })
      .catch(Utils.showError);
  }

  clearFormData = () => {
    this.setState({
      FormData: {
        name: '',
        description: '',
        serializerClassName: '',
        deserializerClassName: ''
      }
    });
    this.refs.addModal.hide();
  }

  render(){
    const {schemaName} = this.props.routeParams;
    const {tableData, FormData} = this.state;
    return <BaseContainer headerContent={[<Link to="/" key="0">All Schemas</Link>, ' / ' +schemaName +' / Serializer & Deserializer']}>
      <div id="add-serializer">
        <button role="button" type="button" className="hb lg success" onClick={this.handleAdd}>
          <i className="fa fa-plus"></i>
        </button>
      </div>
      <div className="wrapper animated fadeIn m-t-md">
        <div className="page-title-box row no-margin white-bg">
          <div className="col-md-12 m-t-md">
          <Table
            className="table table-hover table-bordered"
            noDataText="No records found.">
            <Thead>
              <Th column="name">Name</Th>
              <Th column="description">Description</Th>
              <Th column="serializerClassName">Serializer ClassName</Th>
              <Th column="deserializerClassName">Deserializer ClassName</Th>
            </Thead>
            {tableData.map((entity, i) => {
              const d = entity.serDesPair;
              return <Tr key={i}>
                <Td column="name">{d.name}</Td>
                <Td column="description">{d.description}</Td>
                <Td column="serializerClassName">{d.serializerClassName}</Td>
                <Td column="deserializerClassName">{d.deserializerClassName}</Td>
              </Tr>;
            })}
          </Table>
          <FSModal ref="addModal" data-title={'Add Serializer & Deserializer'} data-resolve={this.addNew} data-reject={this.clearFormData}>
            <SerializerDeserializerForm ref="addSerializerDeserializerForm" FormData={FormData}/>
          </FSModal>
        </div>
        </div>
      </div>
    </BaseContainer>;
  }
}