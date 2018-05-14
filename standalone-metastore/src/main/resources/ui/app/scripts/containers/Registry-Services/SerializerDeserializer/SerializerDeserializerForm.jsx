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
import Form from '../../../libs/form';
import {String, File} from '../../../libs/form/Fields';

export default class SerializerDeserializerForm extends Component {
  constructor(props) {
    super(props);
  }

  render(){
    const {FormData} = this.props;
    return <Form FormData={FormData} ref="Form">
      <String label="Name" value="name" valuePath="name" validation={['required']}/>
      <String label="Description" value="description" valuePath="description" validation={['required']}/>
      <String label="Serializer ClassName" value="serializerClassName" valuePath="serializerClassName" validation={['required']}/>
      <String label="Deserializer ClassName" value="deserializerClassName" valuePath="deserializerClassName" validation={['required']}/>
      <File label="File" value="file" valuePath="file" validation={['required', 'jarFile']}/>
    </Form>;
  }
}