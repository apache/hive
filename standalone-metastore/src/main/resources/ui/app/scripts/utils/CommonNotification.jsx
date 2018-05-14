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
import PropTypes from 'prop-types';
import {notifyTextLimit} from '../utils/Constants';

class CommonNotification extends Component {
  constructor(props) {
    super(props);
    this.state = {
      data: false,
      text: "Read more"
    };
  }
  showMore = () => {
    if (this.state.text === "Read more") {
      this.setState({text: "Hide", data: true});
    } else {
      this.setState({text: "Read more", data: false});
    }
  }

  render() {
    /* flag value         error, info, sucess */
    const {text, data} = this.state;
    const {flag, content} = this.props;
    const initial = content.substr(0, notifyTextLimit);
    const moreText = content.substr(notifyTextLimit);
    const readMoreTag = <a href="javascript:void(0)" onClick={this.showMore}>{text}</a>;
    return (
      <div>
        {initial}
        {(data)
          ? moreText
          : null
}
        <div>
          {(flag === 'error' && moreText.length > 0)
            ? readMoreTag
            : null
}
        </div>
      </div>
    );
  }
}

export default CommonNotification;

CommonNotification.propTypes = {
  flag: PropTypes.string.isRequired,
  content: PropTypes.string
};
