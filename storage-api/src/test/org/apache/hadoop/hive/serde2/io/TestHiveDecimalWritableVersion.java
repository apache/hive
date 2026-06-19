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
package org.apache.hadoop.hive.serde2.io;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Arrays;
import java.util.TreeSet;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.hive.VersionTestBase;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveDecimalV1;
import org.junit.*;

import static org.junit.Assert.*;

public class TestHiveDecimalWritableVersion extends VersionTestBase {

  /*
   * Validation:
   * 1) Substitute class name for "ThisClass".
   * 2) Only public fields and methods are versioned.
   * 3) Methods compare on [non-]static, return type, name, parameter types, exceptions thrown.
   * 4) Fields compare on [non-]static, type, name, value when static
   */
  @Test
  public void testVerifyHiveDecimalWritablePublicMethodsAndFieldsVersions() throws IllegalAccessException {

    Map<Class, String> versionedClassToNameMap = new HashMap<Class, String>();
    versionedClassToNameMap.put(HiveDecimalV1.class, "HiveDecimal");
    versionedClassToNameMap.put(HiveDecimal.class, "HiveDecimal");
    versionedClassToNameMap.put(HiveDecimalWritableV1.class, "HiveDecimalWritable");
    versionedClassToNameMap.put(HiveDecimalWritable.class, "HiveDecimalWritable");

    doVerifyVersions(
        HiveDecimalWritableV1.class, HiveDecimalWritableVersionV1.class,
        HiveDecimalWritable.class, HiveDecimalWritableVersionV2.class,
        versionedClassToNameMap);

  }
}
