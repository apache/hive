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

package org.apache.hadoop.hive.ql.udf.generic;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
/**
 * GenericUDTFParseUrlTuple: this
 *
 */
@Description(name = "parse_url_tuple",
    value = "_FUNC_(url, partname1, partname2, ..., partnameN) - extracts N (N>=1) parts from a URL.\n"
          + "It takes a URL and one or multiple partnames, and returns a tuple. "
          + "All the input parameters and output column types are string.",
    extended = "Partname: HOST, PATH, QUERY, REF, PROTOCOL, AUTHORITY, FILE, USERINFO, QUERY:<KEY_NAME>\n"
             + "Note: Partnames are case-sensitive, and should not contain unnecessary white spaces.\n"
             + "Example:\n"
             + "  > SELECT b.* FROM src LATERAL VIEW _FUNC_(fullurl, 'HOST', 'PATH', 'QUERY', 'QUERY:id') "
             + "b as host, path, query, query_id LIMIT 1;\n"
             + "  > SELECT _FUNC_(a.fullurl, 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'FILE', "
             + " 'AUTHORITY', 'USERINFO', 'QUERY:k1') as (ho, pa, qu, re, pr, fi, au, us, qk1) from src a;")

public class GenericUDTFParseUrlTuple extends GenericUDTF {

  enum PARTNAME {
    HOST, PATH, QUERY, REF, PROTOCOL, AUTHORITY, FILE, USERINFO, QUERY_WITH_KEY, NULLNAME
  };

  private static final Logger LOG = LoggerFactory.getLogger(GenericUDTFParseUrlTuple.class.getName());

  int numCols;    // number of output columns
  String[] paths; // array of pathnames, each of which corresponds to a column
  PARTNAME[] partnames; // mapping from pathnames to enum PARTNAME
  Text[] retCols; // array of returned column values
  Text[] cols;    // object pool of non-null Text, avoid creating objects all the time
  private transient Object[] nullCols; // array of null column values
  private transient ObjectInspector[] inputOIs; // input ObjectInspectors
  boolean pathParsed = false;
  boolean seenErrors = false;
  private transient URL url = null;
  private transient Pattern p = null;
  private transient String lastKey = null;

  @Override
  public void close() throws HiveException {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args)
      throws UDFArgumentException {

    inputOIs = args;
    numCols = args.length - 1;

    if (numCols < 1) {
      throw new UDFArgumentException("parse_url_tuple() takes at least two arguments: " +
          "the url string and a part name");
    }

    for (int i = 0; i < args.length; ++i) {
      if (args[i].getCategory() != ObjectInspector.Category.PRIMITIVE ||
          !args[i].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
        throw new UDFArgumentException("parse_url_tuple()'s arguments have to be string type");
      }
    }

    seenErrors = false;
    pathParsed = false;
    url = null;
    p = null;
    lastKey = null;
    paths = new String[numCols];
    partnames = new PARTNAME[numCols];
    cols = new Text[numCols];
    retCols = new Text[numCols];
    nullCols = new Object[numCols];

    for (int i = 0; i < numCols; ++i) {
      cols[i] = new Text();
      retCols[i] = cols[i];
      nullCols[i] = null;
    }

    // construct output object inspector
    ArrayList<String> fieldNames = new ArrayList<String>(numCols);
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(numCols);
    for (int i = 0; i < numCols; ++i) {
      // column name can be anything since it will be named by UDTF as clause
      fieldNames.add("c" + i);
      // all returned type will be Text
      fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] o) throws HiveException {

    if (o[0] == null) {
      forward(nullCols);
      return;
    }
    // get the path names for the 1st row only
    if (!pathParsed) {
        for (int i = 0;i < numCols; ++i) {
          paths[i] = ((StringObjectInspector) inputOIs[i+1]).getPrimitiveJavaObject(o[i+1]);

          if (paths[i] == null) {
            partnames[i] = PARTNAME.NULLNAME;
          } else if (paths[i].equals("HOST")) {
            partnames[i] = PARTNAME.HOST;
          } else if (paths[i].equals("PATH")) {
            partnames[i] = PARTNAME.PATH;
          } else if (paths[i].equals("QUERY")) {
            partnames[i] = PARTNAME.QUERY;
          } else if (paths[i].equals("REF")) {
            partnames[i] = PARTNAME.REF;
          } else if (paths[i].equals("PROTOCOL")) {
            partnames[i] = PARTNAME.PROTOCOL;
          } else if (paths[i].equals("FILE")) {
            partnames[i] = PARTNAME.FILE;
          } else if (paths[i].equals("AUTHORITY")) {
            partnames[i] = PARTNAME.AUTHORITY;
          } else if (paths[i].equals("USERINFO")) {
            partnames[i] = PARTNAME.USERINFO;
          } else if (paths[i].startsWith("QUERY:")) {
            partnames[i] = PARTNAME.QUERY_WITH_KEY;
            paths[i] = paths[i].substring(6); // update paths[i], e.g., from "QUERY:id" to "id"
          } else {
            partnames[i] = PARTNAME.NULLNAME;
          }
      }
      pathParsed = true;
    }

    String urlStr = ((StringObjectInspector) inputOIs[0]).getPrimitiveJavaObject(o[0]);
    if (urlStr == null) {
      forward(nullCols);
      return;
    }

    try {
      String ret = null;
      url = new URL(urlStr);
      for (int i = 0; i < numCols; ++i) {
        ret = evaluate(url, i);
        if (ret == null) {
          retCols[i] = null;
        } else {
          if (retCols[i] == null) {
            retCols[i] = cols[i]; // use the object pool rather than creating a new object
          }
          retCols[i].set(ret);
        }
      }

      forward(retCols);
      return;
    } catch (MalformedURLException e) {
      // parsing error, invalid url string
      if (!seenErrors) {
        LOG.error("The input is not a valid url string: " + urlStr + ". Skipping such error messages in the future.");
        seenErrors = true;
      }
      forward(nullCols);
      return;
    }
  }

  @Override
  public String toString() {
    return "parse_url_tuple";
  }

  private String evaluate(URL url, int index) {
    if (url == null || index < 0 || index >= partnames.length) {
      return null;
    }

    switch (partnames[index]) {
      case HOST          : return url.getHost();
      case PATH          : return url.getPath();
      case QUERY         : return url.getQuery();
      case REF           : return url.getRef();
      case PROTOCOL      : return url.getProtocol();
      case FILE          : return url.getFile();
      case AUTHORITY     : return url.getAuthority();
      case USERINFO      : return url.getUserInfo();
      case QUERY_WITH_KEY: return evaluateQuery(url.getQuery(), paths[index]);
      case NULLNAME:
      default            : return null;
    }
  }

  private String evaluateQuery(String query, String key) {
    if (query == null || key == null) {
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
