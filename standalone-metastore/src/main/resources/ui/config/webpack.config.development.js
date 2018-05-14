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

const merge = require('webpack-merge');
const webpack = require('webpack');
const config = require('./webpack.config.base');
const path = require('path');
const extractTextPlugin = require("extract-text-webpack-plugin");


const GLOBALS = {
  'process.env': {
    'NODE_ENV': JSON.stringify('development')
  },
  __DEV__: JSON.stringify(JSON.parse(process.env.DEBUG || 'true'))
};

module.exports = merge(config, {
  debug: true,
  cache: true,
  //devtool: 'cheap-module-eval-source-map',
  devtool: 'inline-source-map',
  output: {
    filename: 'ui/js/[name].js',
    path: path.resolve(__dirname, '../public/assets'),
    publicPath: '/'
  },
  entry: {
    application: [
      'webpack-hot-middleware/client',
      'react-hot-loader/patch',
      path.join(__dirname, '../app/scripts/main')
    ],
    vendor: ['react', 'react-dom', 'react-router']
  },
  plugins: [
    new webpack.optimize.CommonsChunkPlugin({
      name: 'vendor',
      filename: 'ui/js/vendor.bundle.js',
      minChunks: Infinity
    }),
    new extractTextPlugin("[name].css"),
    new webpack.HotModuleReplacementPlugin(),
    new webpack.DefinePlugin(GLOBALS)
  ],
  module: {
    loaders: [{
      test: /\.css$/,
      loader: 'style-loader'
    }, {
      test: /\.css$/,
      loader: 'css-loader',
      query: {
        //modules: true,
        localIdentName: '[name]__[local]___[hash:base64:5]'
      }
    }, ]
  }
});
