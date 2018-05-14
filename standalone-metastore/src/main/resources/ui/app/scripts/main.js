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

require('file?name=[name].[ext]!../../index.html'); //for production build

import React, {Component} from 'react';
import {render} from 'react-dom';
import debug from 'debug';
import 'babel-polyfill';
import App from './app';
import {AppContainer} from 'react-hot-loader';

import '../styles/css/font-awesome.min.css';
import 'animate.css/animate.css';
import '../styles/css/bootstrap.css';
import 'react-select/dist/react-select.css';
import 'react-bootstrap-switch/dist/css/bootstrap3/react-bootstrap-switch.min.css';
import '../styles/css/toastr.min.css';
import 'codemirror/lib/codemirror.css';
import 'codemirror/addon/lint/lint.css';
import 'gitgraph.js/build/gitgraph.css';
import './libs/jsdifflib/diffview.css';
import '../styles/css/style.css';

render(
  <AppContainer>
    <App/>
  </AppContainer>, document.getElementById('app_container'));

if (module.hot) {
  module.hot.accept('./app', () => {
    const NextApp = require('./app').default;
    render(
      <AppContainer>
        <NextApp/>
      </AppContainer>, document.getElementById('app_container'));
  });
}
