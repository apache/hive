package org.apache.hadoop.hive.ql.util;
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

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public class NameCaseUtil {

    private static final String SEPARATOR = "_";

    private NameCaseUtil() {
    }

    public static String camelToUnderScoreName(String s){
        if (StringUtils.isEmpty(s)) {
            return s;
        }
        String[] partName = s.replaceAll("[A-Z]", "_$0").split(SEPARATOR);
        System.out.println(Arrays.toString(partName)) ;
        return String.join(SEPARATOR, partName).toLowerCase().replaceAll("^_","");
    }

    public static String underScoreToCamelName(String s) {
        if (StringUtils.isEmpty(s)) {
            return s;
        }

        String[] partName = s.split(SEPARATOR);
        for (int i = 0; i < partName.length; i++) {
             partName[i] = capitalize(partName[i]);
        }
        return String.join("",partName);
    }
    public static String capitalize(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return str;
        }
        return new StringBuilder(strLen)
                .append(Character.toTitleCase(str.charAt(0)))
                .append(str.substring(1).toLowerCase())
                .toString();
    }
}
