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

package org.apache.hadoop.hive.ql.udf;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDF to extract specfic parts from URL For example,
 * parse_url('http://facebook.com/path/p1.php?query=1', 'HOST') will return
 * 'facebook.com' For example,
 * parse_url('http://facebook.com/path/p1.php?query=1', 'PATH') will return
 * '/path/p1.php' parse_url('http://facebook.com/path/p1.php?query=1', 'QUERY')
 * will return 'query=1'
 * parse_url('http://facebook.com/path/p1.php?query=1#Ref', 'REF') will return
 * 'Ref' parse_url('http://facebook.com/path/p1.php?query=1#Ref', 'PROTOCOL')
 * will return 'http' Possible values are
 * HOST,PATH,QUERY,REF,PROTOCOL,AUTHORITY,FILE,USERINFO Also you can get a value
 * of particular key in QUERY, using syntax QUERY:<KEY_NAME> eg: QUERY:k1.
 */
@Description(name = "parse_url",
    value = "_FUNC_(url, partToExtract[, key]) - extracts a part from a URL",
    extended = "Parts: HOST, PATH, QUERY, REF, PROTOCOL, AUTHORITY, FILE, "
    + "USERINFO\nkey specifies which query to extract\n"
    + "Example:\n"
    + "  > SELECT _FUNC_('http://facebook.com/path/p1.php?query=1', "
    + "'HOST') FROM src LIMIT 1;\n"
    + "  'facebook.com'\n"
    + "  > SELECT _FUNC_('http://facebook.com/path/p1.php?query=1', "
    + "'QUERY') FROM src LIMIT 1;\n"
    + "  'query=1'\n"
    + "  > SELECT _FUNC_('http://facebook.com/path/p1.php?query=1', "
    + "'QUERY', 'query') FROM src LIMIT 1;\n" + "  '1'")
public class UDFParseUrl extends UDF {
  private String lastUrlStr = null;
  private URL url = null;
  private Pattern p = null;
  private String lastKey = null;

  public UDFParseUrl() {
  }

  public String evaluate(String urlStr, String partToExtract) {
    if (urlStr == null || partToExtract == null) {
      return null;
    }

    if (lastUrlStr == null || !urlStr.equals(lastUrlStr)) {
      try {
        url = new URL(urlStr);
      } catch (Exception e) {
        return null;
      }
    }
    lastUrlStr = urlStr;

    if (partToExtract.equals("HOST")) {
      return url.getHost();
    }
    if (partToExtract.equals("PATH")) {
      return url.getPath();
    }
    if (partToExtract.equals("QUERY")) {
      return url.getQuery();
    }
    if (partToExtract.equals("REF")) {
      return url.getRef();
    }
    if (partToExtract.equals("PROTOCOL")) {
      return url.getProtocol();
    }
    if (partToExtract.equals("FILE")) {
      return url.getFile();
    }
    if (partToExtract.equals("AUTHORITY")) {
      return url.getAuthority();
    }
    if (partToExtract.equals("USERINFO")) {
      return url.getUserInfo();
    }

    return null;
  }

  public String evaluate(String urlStr, String partToExtract, String key) {
    if (!partToExtract.equals("QUERY")) {
      return null;
    }

    String query = this.evaluate(urlStr, partToExtract);
    if (query == null) {
      return null;
    }

    if (!key.equals(lastKey)) {
      p = Pattern.compile("(&|^)" + key + "=([^&]*)");
    }

    lastKey = key;
    Matcher m = p.matcher(query);
    if (m.find()) {
      return m.group(2);
    }
    return null;
  }
}
