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

const path = require('path');
const merge = require('webpack-merge');
const webpack = require('webpack');
const ExtractTextPlugin = require('extract-text-webpack-plugin');
const CopyWebpackPlugin = require('copy-webpack-plugin');
const config = require('./webpack.config.base');

const GLOBALS = {
  'process.env': {
    'NODE_ENV': JSON.stringify('production')
  },
  __DEV__: JSON.stringify(JSON.parse(process.env.DEBUG || 'false'))
};

module.exports = merge(config, {
  debug: false,
  devtool: 'cheap-module-source-map',
  entry: {
    application: 'main.js',
    vendor: ['react', 'react-dom', 'react-router']
  },
  plugins: [
    new CopyWebpackPlugin([{
      from: path.join(__dirname, '../app/styles/img'),
      to: 'styles/img'
    }]),
    // Avoid publishing files when compilation fails
    new webpack.NoErrorsPlugin(),
    new webpack.DefinePlugin(GLOBALS),
    new webpack.optimize.DedupePlugin(),
    new webpack.optimize.UglifyJsPlugin({
      compress: {
        warnings: false,
        'screw_ie8': true
      },
      output: {
        comments: false
      },
      sourceMap: false
    }),
    new webpack.LoaderOptionsPlugin({
      minimize: true,
      debug: false
    }),
    new ExtractTextPlugin({
      filename: 'styles/css/style.css',
      allChunks: true
    })
  ],
  module: {
    noParse: /\.min\.js$/,
    loaders: [
      // Sass
      {
        test: /\.scss$/,
        include: [
          /src\/client\/assets\/javascripts/,
          /src\/client\/assets\/styles/,
          /src\/client\/scripts/
        ],
        loader: ExtractTextPlugin.extract({
          fallbackLoader: 'style',
          loader: [{
              loader: 'css',
              query: {
                sourceMap: true
              }
            },
            'postcss',
            {
              loader: 'sass',
              query: {
                outputStyle: 'compressed'
              }
            }
          ]
        })
      },
      // Sass + CSS Modules
      // {
      //   test: /\.scss$/,
      //   include: /src\/client\/assets\/javascripts/,
      //   loader: ExtractTextPlugin.extract({
      //     fallbackLoader: 'style',
      //     loader: [
      //       {
      //         loader: 'css',
      //         query: {
      //           modules: true,
      //           importLoaders: 1,
      //           localIdentName: '[path][name]__[local]--[hash:base64:5]'
      //         }
      //       },
      //       'postcss',
      //       { loader: 'sass', query: { outputStyle: 'compressed' } }
      //     ]
      //   })
      // },
      // CSS
      {
        test: /\.css$/,
        loader: ExtractTextPlugin.extract({
          fallbackLoader: 'style',
          loader: ['css', 'postcss'],
          publicPath: '../../'
        })
      }
    ]
  },
});
