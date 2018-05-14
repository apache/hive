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

import ReactCodemirror from 'react-codemirror';
import fetch from 'isomorphic-fetch';
// import pace from 'pace-progress';

ReactCodemirror.prototype.componentWillReceiveProps = function(nextProps) {

  if (this.codeMirror && nextProps.value !== undefined && this.codeMirror.getValue() != nextProps.value) {
    this.codeMirror.setValue(nextProps.value);
  }
  if (typeof nextProps.options === 'object') {
    for (var optionName in nextProps.options) {
      if (nextProps.options.hasOwnProperty(optionName)) {
        this.codeMirror.setOption(optionName, nextProps.options[optionName]);
      }
    }
  }
};


export function CustomFetch() {
  // pace.start()
  const _promise = fetch.apply(this, arguments);
  _promise.then(function() {
    // pace.stop()
  }, function() {
    // pace.stop()
  });
  return _promise;
};
