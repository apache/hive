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
import Header from '../components/Header';
import Footer from '../components/Footer';
import {Link} from 'react-router';
import {Confirm} from '../components/FSModal';

export default class BaseContainer extends Component {

  constructor(props) {
    super(props);
    // Operations usually carried out in componentWillMount go here
    this.state = {};
  }

  render() {
    const routes = this.props.routes || [
      {
        path: "/",
        name: 'Home'
      }
    ];
    return (
      <div>
        <Header onLandingPage={this.props.onLandingPage} headerContent={this.props.headerContent}/>
        <section className={this.props.onLandingPage === "true"
          ? "landing-wrapper container"
          : "container wrapper animated fadeIn"}>
          {this.props.children}
        </section>
        <Confirm ref="Confirm"/>
      </div>
    );
  }
}
