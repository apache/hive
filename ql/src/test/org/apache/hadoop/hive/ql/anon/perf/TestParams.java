/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.anon.perf;

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.FileType;

import java.util.ArrayList;
import java.util.List;


public final class TestParams {

  public int id;
  public int totalMessages = -1;
  public int numUniqueUsers;
  public int allToPiiRatio;
  public int piiMsgFieldLen;
  public int numFiles;
  public ColumnInternalFormat internalFormat;
  public FileType fileType;
  public int numKeys;

  private static final List<TestParams> lst = new ArrayList<>();

  public static List<TestParams> getLst() {
    return lst;
  }

  static {
    int id = 1;
    for (final FileType fileType : TestData.fileTypes) {
      for (final int numFiles : TestData.numFiles) {
        for (final int numKeys : TestData.numKeys) {
          for (final int unique : TestData.uniques) {
            for (final int ratio : TestData.ratios) {
              for (final int fieldLen : TestData.fieldLens) {
                for (final ColumnInternalFormat internalFormat : ColumnInternalFormat.values()) {
                  TestParams testParams = new TestParams();
                  testParams.totalMessages = TestData.totalMessages;
                  testParams.allToPiiRatio = ratio;
                  testParams.numUniqueUsers = unique;
                  testParams.piiMsgFieldLen = fieldLen;
                  testParams.numKeys = numKeys;
                  testParams.numFiles = numFiles;
                  testParams.internalFormat = internalFormat;
                  testParams.fileType = fileType;
                  testParams.id = id;
                  lst.add(testParams);
                  id++;
                }
              }
            }
          }
        }
      }
    }
  }
}
