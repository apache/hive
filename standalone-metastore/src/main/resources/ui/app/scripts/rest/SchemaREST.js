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
import {baseUrl} from '../utils/Constants';
import {CustomFetch} from '../utils/Overrides';
import {checkStatus} from '../utils/Utils';

const SchemaREST = {
  postSchema(options) {
    options = options || {};
    options.method = options.method || 'POST';
    options.headers = options.headers || {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      '_throwErrorIfExists': 'true'
    };
    options.credentials = 'same-origin';
    return fetch(baseUrl + 'schemaregistry/schemas', options)
      .then(checkStatus);
  },
  getAllSchemas(sortBy, options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    let url = baseUrl + 'schemaregistry/schemas';
    if(sortBy){
      url += '/?_orderByFields='+sortBy;
    }
    return fetch(url, options)
      .then((response) => {
        return response.json();
      });
  },
  getAggregatedSchema(schemaName, options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    let url = baseUrl + `schemaregistry/schemas/${schemaName}/aggregated`;
    return fetch(url, options)
      .then(checkStatus);
  },
  getAggregatedSchemas(sortBy, options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    let url = baseUrl + 'schemaregistry/schemas/aggregated';
    if(sortBy){
      url += '/?_orderByFields='+sortBy;
    }
    return fetch(url, options)
      .then((response) => {
        return response.json();
      });
  },
  searchAggregatedSchemas(sortBy, searchStr, options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    let url = baseUrl + 'schemaregistry/search/schemas/aggregated?';
    const params = [];
    if(sortBy){
      params.push('_orderByFields='+sortBy);
    }
    params.push('name='+searchStr);
    url += params.join('&');
    return fetch(url, options)
      .then(checkStatus);
  },
  getSchemaInfo(name, options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    name = encodeURIComponent(name);
    return fetch(baseUrl + 'schemaregistry/schemas/' + name, options)
      .then((response) => {
        return response.json();
      });
  },
  getSchemaVersions(name, options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    name = encodeURIComponent(name);
    return fetch(baseUrl + 'schemaregistry/schemas/' + name + '/versions', options)
      .then((response) => {
        return response.json();
      });
  },
  getLatestVersion(name, options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    name = encodeURIComponent(name);
    return fetch(baseUrl+'schemaregistry/schemas/'+name+'/versions/latest', options)
      .then( (response) => {
        return response.json();
      });
  },
  postVersion(name, options, branchName) {
    options = options || {};
    options.method = options.method || 'POST';
    options.headers = options.headers || {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    };
    options.credentials = 'same-origin';
    name = encodeURIComponent(name);
    branchName = branchName || 'MASTER';
    return fetch(baseUrl + 'schemaregistry/schemas/' + name + '/versions?branch='+branchName, options)
      .then((response) => {
        return response.json();
      });
  },
  getSchemaProviders(options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    return fetch(baseUrl + 'schemaregistry/schemaproviders', options)
      .then(checkStatus);
  },
  getCompatibility(name, options) {
    options = options || {};
    options.method = options.method || 'POST';
    options.headers = options.headers || {
      'Content-Type' : 'application/json',
      'Accept' : 'application/json'
    };
    options.credentials = 'same-origin';
    name = encodeURIComponent(name);
    return fetch(baseUrl+'schemaregistry/schemas/'+name+'/compatibility', options)
      .then(checkStatus);
  },
  getSchemaVersionStateMachine(options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    return fetch(baseUrl + 'schemaregistry/schemas/versions/statemachine', options)
      .then(checkStatus);
  },
  changeStateOfVersion(verId, stateId, options){
    options = options || {};
    options.method = options.method || 'POST';
    options.headers = options.headers || {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    };
    options.credentials = 'same-origin';
    return fetch(baseUrl + 'schemaregistry/schemas/versions/'+verId+'/state/'+stateId, options)
      .then(checkStatus);
  },
  getBranches(schemaName, options) {
    options = options || {};
    options.method = options.method || 'GET';
    options.credentials = 'same-origin';
    return fetch(baseUrl + `schemaregistry/schemas/${schemaName}/branches`, options)
      .then(checkStatus);
  },
  forkNewBranch(verId, options){
    options = options || {};
    options.method = options.method || 'POST';
    options.headers = options.headers || {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    };
    options.credentials = 'same-origin';
    return fetch(baseUrl + `schemaregistry/schemas/versionsById/${verId}/branch`, options)
      .then(checkStatus);
  },
  mergeBranch(verId, options){
    options = options || {};
    options.method = options.method || 'POST';
    options.headers = options.headers || {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    };
    options.credentials = 'same-origin';
    return fetch(baseUrl + `schemaregistry/schemas/${verId}/merge`, options)
      .then(checkStatus);
  },
  deleteBranch(branchId, options){
    options = options || {};
    options.method = options.method || 'DELETE';
    options.headers = options.headers || {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    };
    options.credentials = 'same-origin';
    return fetch(baseUrl + `schemaregistry/schemas/branch/${branchId}`, options)
      .then((res) => {
        return checkStatus(res, 'DELETE');
      });
  }
};

export default SchemaREST;
