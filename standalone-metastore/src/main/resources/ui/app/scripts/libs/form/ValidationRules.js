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

let ValidationRules = {
  required: (value, form, component) => {
    if (!value) {
      return 'Required!';
    } else {
      if (value instanceof Array) {
        if (value.length === 0) {
          return 'Required!';
        }
      } else if (value == '' || (typeof value == 'string' && value.trim() == '') || _.isUndefined(value)) {
        return 'Required!';
      } else {
        return '';
      }
    }
  },
  email: (value, form, component) => {
    if (!value.trim()) {
      return false;
    } else {
      if (value instanceof Array) {
        return false;
      } else if (value == '' || (typeof value == 'string' && value.trim() == '') || _.isUndefined(value)) {
        return false;
      } else {
        let result = '';
        const pattern = /[a-z0-9](\.?[a-z0-9_-]){0,}@[a-z0-9-]+\.([a-z]{1,6}\.)?[a-z]{2,6}$/;
        pattern.test(value) ? result : result = "Invalid Email";
        return result;
      }
    }
  },
  jarFile: (value, form, component) => {
    let result;
    if(!(value.type == "application/x-java-archive" || value.type == "application/java-archive")){
      result = 'Invalid File Type';
    }
    return result;
  }
};

export default ValidationRules;
