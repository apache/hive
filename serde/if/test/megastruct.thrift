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

/**
 * MetaStruct intends to stress test Hive's thrift support by having all
 * sorts of crazy but valid field types. Please add new fields if you find
 * a case that's not handled correctly.
 */

namespace java org.apache.hadoop.hive.serde2.thrift.test

enum MyEnum {
  LLAMA = 1,
  ALPACA = 2
}

struct MiniStruct {
  1: optional string my_string,
  2: optional MyEnum my_enum
}

struct MegaStruct {
   1: optional bool my_bool,
   2: optional byte my_byte,
   3: optional i16 my_16bit_int,
   4: optional i32 my_32bit_int,
   5: optional i64 my_64bit_int,
   6: optional double my_double,
   7: optional string my_string,
   8: optional binary my_binary,
   9: optional map<string, string> my_string_string_map,
  10: optional map<string, MyEnum> my_string_enum_map,
  11: optional map<MyEnum, string> my_enum_string_map,
  12: optional map<MyEnum, MiniStruct> my_enum_struct_map,
  13: optional map<MyEnum, list<string>> my_enum_stringlist_map,
  14: optional map<MyEnum, list<MiniStruct>> my_enum_structlist_map,
  15: optional list<string> my_stringlist,
  16: optional list<MiniStruct> my_structlist,
  17: optional list<MyEnum> my_enumlist,
  18: optional set<string> my_stringset,
  19: optional set<MyEnum> my_enumset,
  20: optional set<MiniStruct> my_structset
}
