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

import fetch from 'isomorphic-fetch';
import {
  baseUrl
} from '../utils/Constants';
import {checkStatus} from '../utils/Utils';

const SerializerDeserializerREST = {
  getSerDess(schemaName, options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    return fetch(baseUrl + 'schemaregistry/schemas/'+ schemaName +'/serdes', options)
      .then(checkStatus);
  },
  postSerDes(options) {
    options = options || {};
    options.method = options.method || 'POST';
    options.credentials = 'same-origin';
    options.headers = options.headers || {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    };
    return fetch(baseUrl + 'schemaregistry/serdes', options)
      .then(checkStatus);
  },
  getSerDes(id, options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    return fetch(baseUrl + 'schemaregistry/serdes/'+id, options)
      .then(checkStatus);
  },
  postMapping(schemaName, serDesId, options) {
    options = options || {};
    options.method = options.method || 'POST';
    options.credentials = 'same-origin';
    return fetch(baseUrl + 'schemaregistry/schemas/'+schemaName+'/mapping/'+serDesId, options)
      .then(checkStatus);
  }
};

export default SerializerDeserializerREST;
