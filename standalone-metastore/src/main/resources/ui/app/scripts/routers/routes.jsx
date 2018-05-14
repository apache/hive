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

import React from 'react';
import {Router, Route, hashHistory, browserHistory, IndexRoute} from 'react-router';

import SchemaRegContainer from '../containers/Registry-Services/SchemaRegistry/SchemaRegistryContainer';
import Serdes from '../containers/Registry-Services/SerializerDeserializer/SerializerDeserializerContainer';

const onEnter = (nextState, replace, callback) => {
  callback();
};

export default(
  <Route path="/" component={null} name="Home" onEnter={onEnter}>
    <IndexRoute name="Schema Registry" component={SchemaRegContainer} onEnter={onEnter}/>
    <Route path="schema-registry" name="Schema Registry" onEnter={onEnter}>
      <IndexRoute component={SchemaRegContainer} onEnter={onEnter}/>
    </Route>
    <Route path="schemas" name="schema" onEnter={onEnter}>
      <IndexRoute component={null} onEnter={onEnter}/>
      <Route path=":schemaName/serdes" name="serdes" component={Serdes} onEnter={onEnter}/>
    </Route>
  </Route>
);
