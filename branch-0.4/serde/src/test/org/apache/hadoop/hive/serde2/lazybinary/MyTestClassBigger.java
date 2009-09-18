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
package org.apache.hadoop.hive.serde2.lazybinary;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.binarysortable.MyTestInnerStruct;

public class MyTestClassBigger {
  Byte myByte;
  Short myShort;
  Integer myInt;
  Long myLong;
  Float myFloat;
  Double myDouble;
  String myString;
  MyTestInnerStruct myStruct;
  List<Integer> myList;
  Map<String, List<MyTestInnerStruct>> myMap;
  
  public MyTestClassBigger() {  
  }
  
  public MyTestClassBigger(Byte b, Short s, Integer i, Long l, 
      Float f, Double d, String st, MyTestInnerStruct is, 
      List<Integer> li, Map<String, List<MyTestInnerStruct>> mp) {
    this.myByte = b;
    this.myShort = s;
    this.myInt = i;
    this.myLong = l;
    this.myFloat = f;
    this.myDouble = d;
    this.myString = st;
    this.myStruct = is;
    this.myList = li;
    this.myMap  = mp;
  }
}
