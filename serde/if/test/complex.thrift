/**
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

namespace java org.apache.hadoop.hive.serde2.thrift.test

struct IntString {
  1: i32  myint;
  2: string myString;
  3: i32  underscore_int;
}

struct Complex {
  1: i32 aint;
  2: string aString;
  3: list<i32> lint;
  4: list<string> lString;
  5: list<IntString> lintString;
  6: map<string, string> mStringString;
}
