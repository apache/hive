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

package org.apache.hadoop.hive.contrib.serde2.s3;

import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ReflectionStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class S3LogDeserializer implements Deserializer {

  public static final Log LOG = LogFactory.getLog(S3LogDeserializer.class
      .getName());

  static {
    StackTraceElement[] sTrace = new Exception().getStackTrace();
    sTrace[0].getClassName();
  }

  private ObjectInspector cachedObjectInspector;

  @Override
  public String toString() {
    return "S3ZemantaDeserializer[]";
  }

  public S3LogDeserializer() throws SerDeException {
  }

  // This regex is a bit lax in order to compensate for lack of any escaping
  // done by Amazon S3 ... for example useragent string can have double quotes
  // inside!
  static Pattern regexpat = Pattern
      .compile("(\\S+) (\\S+) \\[(.*?)\\] (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"(.+)\" (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"(.*)\" \"(.*)\"");
  // static Pattern regexrid = Pattern.compile("x-id=([-0-9a-f]{36})");
  // static SimpleDateFormat dateparser = new
  // SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss ZZZZZ");

  S3LogStruct deserializeCache = new S3LogStruct();

  public void initialize(Configuration job, Properties tbl)
      throws SerDeException {

    cachedObjectInspector = ObjectInspectorFactory
        .getReflectionObjectInspector(S3LogStruct.class,
            ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

    LOG.debug(getClass().getName() + ": initialized");
  }

  public static Integer toInt(String s) {
    if (s.compareTo("-") == 0) {
      return null;
    } else {
      return Integer.valueOf(s);
    }
  }

  public static Object deserialize(S3LogStruct c, String row) throws Exception {
    Matcher match = regexpat.matcher(row);
    int t = 1;
    try {
      match.matches();
      c.bucketowner = match.group(t++);
      c.bucketname = match.group(t++);
    } catch (Exception e) {
      throw new SerDeException("S3 Log Regex did not match:" + row, e);
    }
    c.rdatetime = match.group(t++);

    // Should we convert the datetime to the format Hive understands by default
    // - either yyyy-mm-dd HH:MM:SS or seconds since epoch?
    // Date d = dateparser.parse(c.rdatetime);
    // c.rdatetimeepoch = d.getTime() / 1000;

    c.rip = match.group(t++);
    c.requester = match.group(t++);
    c.requestid = match.group(t++);
    c.operation = match.group(t++);
    c.rkey = match.group(t++);
    c.requesturi = match.group(t++);
    // System.err.println(c.requesturi);
    /*
     * // Zemanta specific data extractor try { Matcher m2 =
     * regexrid.matcher(c.requesturi); m2.find(); c.rid = m2.group(1); } catch
     * (Exception e) { c.rid = null; }
     */
    c.httpstatus = toInt(match.group(t++));
    c.errorcode = match.group(t++);
    c.bytessent = toInt(match.group(t++));
    c.objsize = toInt(match.group(t++));
    c.totaltime = toInt(match.group(t++));
    c.turnaroundtime = toInt(match.group(t++));
    c.referer = match.group(t++);
    c.useragent = match.group(t++);

    return (c);
  }

  public Object deserialize(Writable field) throws SerDeException {
    String row = null;
    if (field instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) field;
      try {
        row = Text.decode(b.get(), 0, b.getSize());
      } catch (CharacterCodingException e) {
        throw new SerDeException(e);
      }
    } else if (field instanceof Text) {
      row = field.toString();
    }
    try {
      deserialize(deserializeCache, row);
      return deserializeCache;
    } catch (ClassCastException e) {
      throw new SerDeException(this.getClass().getName()
          + " expects Text or BytesWritable", e);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    System.err.println("This is only a test run");
    try {
      S3LogDeserializer serDe = new S3LogDeserializer();
      Configuration conf = new Configuration();
      Properties tbl = new Properties();
      // Some nasty examples that show how S3 log format is broken ... and to
      // test the regex
      // These are all sourced from genuine S3 logs
      // Text sample = new
      // Text("04ff331638adc13885d6c42059584deabbdeabcd55bf0bee491172a79a87b196 img.zemanta.com [09/Apr/2009:22:00:07 +0000] 190.225.84.114 65a011a29cdf8ec533ec3d1ccaae921c F4FC3FEAD8C00024 REST.GET.OBJECT pixy.gif \"GET /pixy.gif?x-id=23d25db1-160b-48bb-a932-e7dc1e88c321 HTTP/1.1\" 304 - - 828 3 - \"http://www.viamujer.com/2009/03/horoscopo-acuario-abril-mayo-y-junio-2009/\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)\"");
      // Text sample = new
      // Text("04ff331638adc13885d6c42059584deabbdeabcd55bf0bee491172a79a87b196 img.zemanta.com [09/Apr/2009:22:19:49 +0000] 60.28.204.7 65a011a29cdf8ec533ec3d1ccaae921c 7D87B6835125671E REST.GET.OBJECT pixy.gif \"GET /pixy.gif?x-id=b50a4544-938b-4a63-992c-721d1a644b28 HTTP/1.1\" 200 - 828 828 4 3 \"\" \"ZhuaXia.com\"");
      // Text sample = new
      // Text("04ff331638adc13885d6c42059584deabbdeabcd55bf0bee491172a79a87b196 static.zemanta.com [09/Apr/2009:23:12:39 +0000] 65.94.12.181 65a011a29cdf8ec533ec3d1ccaae921c EEE6FFE9B9F9EA29 REST.HEAD.OBJECT readside/loader.js%22+defer%3D%22defer \"HEAD /readside/loader.js\"+defer=\"defer HTTP/1.0\" 403 AccessDenied 231 - 7 - \"-\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)\"");
      Text sample = new Text(
          "04ff331638adc13885d6c42059584deabbdeabcd55bf0bee491172a79a87b196 img.zemanta.com [10/Apr/2009:05:34:01 +0000] 70.32.81.92 65a011a29cdf8ec533ec3d1ccaae921c F939A7D698D27C63 REST.GET.OBJECT reblog_b.png \"GET /reblog_b.png?x-id=79ca9376-6326-41b7-9257-eea43d112eb2 HTTP/1.0\" 200 - 1250 1250 160 159 \"-\" \"Firefox 0.8 (Linux)\" useragent=\"Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.6) Gecko/20040614 Firefox/0.8\"");
      serDe.initialize(conf, tbl);
      Object row = serDe.deserialize(sample);
      System.err.println(serDe.getObjectInspector().getClass().toString());
      ReflectionStructObjectInspector oi = (ReflectionStructObjectInspector) serDe
          .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      for (int i = 0; i < fieldRefs.size(); i++) {
        System.err.println(fieldRefs.get(i).toString());
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        if (fieldData == null) {
          System.err.println("null");
        } else {
          System.err.println(fieldData.toString());
        }
      }

    } catch (Exception e) {
      System.err.println("Caught: " + e);
      e.printStackTrace();
    }

  }

}
