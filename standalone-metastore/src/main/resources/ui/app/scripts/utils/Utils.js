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

import _ from 'lodash';
import React from 'react';
import moment from 'moment';
import FSReactToastr from '../components/FSReactToastr';
import {toastOpt} from './Constants';
import CommonNotification from './CommonNotification';

const isValidJson = function(obj) {
  try {
    const valid = JSON.parse(obj);
    return true;
  } catch (e) {
    return false;
  }
};

const sortArray = function(sortingArr, keyName, ascendingFlag){
  if (ascendingFlag){
    return sortingArr.sort(function(a, b) {
      if(a[keyName] < b[keyName]){ return -1; }
      if(a[keyName] > b[keyName]){ return 1; }
      return 0;
    });
  }
  else {
    return sortingArr.sort(function(a, b) {
      if(b[keyName] < a[keyName]){ return -1; }
      if(b[keyName] > a[keyName]) { return 1; }
      return 0;
    });
  }
};

const splitTimeStamp = function(date){
  const currentDT = moment(new Date());
  const createdDT = moment(date);
  const dateObj = moment.duration(currentDT.diff(createdDT));
  return  ((dateObj._data.days === 0)
    ? '': dateObj._data.days+'d ')
     +((dateObj._data.days === 0 && dateObj._data.hours === 0 )
       ? '' : dateObj._data.hours+'h ')
        + ((dateObj._data.days === 0 && dateObj._data.hours === 0 && dateObj._data.minutes === 0)
          ? '' : dateObj._data.minutes+'m ')
            + dateObj._data.seconds+'s ago';
};

const getDateTimeLabel = function(date){
  const time = date.toLocaleTimeString();
  const arr = date.toString().split(' ');
  return (arr[1]+'-'+arr[2]+'-'+arr[3]+' at ' + time);
};

const ellipses = function(string, tagWidth) {
  if (!string || tagWidth === undefined) {
    return;
  }
  return string.length > (tagWidth/10) ? `${string.substr(0, tagWidth/10)}...` : string;
};

const showError = function(err) {
  if(err.response){
    err.response.then((res) => {
      FSReactToastr.error(
        <CommonNotification flag="error" content={res.responseMessage}/>, '', toastOpt
      );
    });
  }else{
    console.error(err);
  }
};

export function checkStatus(response, method) {
  if (response.status >= 200 && response.status < 300) {
    if(method !== 'DELETE'){
      return response.json();
    }else{
      return response;
    }
  } else {
    const error = new Error(response.statusText);
    error.response = response.json();
    throw error;
  }
};

export class StateMachine {
  constructor(obj = {}){
    this.states = obj.states;
    this.transitions = obj.transitions;
  }
  setStates(obj = {}){
    this.states = obj.states;
    this.transitions = obj.transitions;
  }
  getStateById(id){
    return _.find(this.states, {id: id});
  }
  getStateByName(name){
    return _.find(this.states, {name: name});
  }
  getTransitionStateOptions(id){
    var options = _.filter(this.transitions, (t) => {
      return t.sourceStateId == id;
    });
    return options;
  }
}

export default {
  isValidJson,
  sortArray,
  splitTimeStamp,
  getDateTimeLabel,
  ellipses,
  showError
};
