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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton.tool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.common.LogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.hcatalog.templeton.UgiFactory;

/**
 * General utility methods.
 */
public class TempletonUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TempletonUtils.class);

  /**
   * Is the object non-empty?
   */
  public static boolean isset(String s) {
    return (s != null) && (s.length() > 0);
  }

  /**
   * Is the object non-empty?
   */
  public static boolean isset(char ch) {
    return (ch != 0);
  }

  /**
   * Is the object non-empty?
   */
  public static <T> boolean isset(T[] a) {
    return (a != null) && (a.length > 0);
  }


  /**
   * Is the object non-empty?
   */
  public static <T> boolean isset(Collection<T> col) {
    return (col != null) && (!col.isEmpty());
  }

  /**
   * Is the object non-empty?
   */
  public static <K, V> boolean isset(Map<K, V> col) {
    return (col != null) && (!col.isEmpty());
  }

  //looking for map 100% reduce 100%
  public static final Pattern JAR_COMPLETE = Pattern.compile(" map \\d+%\\s+reduce \\d+%$");
  public static final Pattern PIG_COMPLETE = Pattern.compile(" \\d+% complete$");
  //looking for map = 100%,  reduce = 100%
  public static final Pattern HIVE_COMPLETE = Pattern.compile(" map = (\\d+%),\\s+reduce = (\\d+%).*$");
  /**
   * Hive on Tez produces progress report that looks like this
   * Map 1: -/-	Reducer 2: 0/1
   * Map 1: -/-	Reducer 2: 0(+1)/1
   * Map 1: -/-	Reducer 2: 1/1
   *
   * -/- means there are no tasks (yet)
   * 0/1 means 1 total tasks, 0 completed
   * 1(+2)/3 means 3 total, 1 completed and 2 running
   *
   * HIVE-8495, in particular https://issues.apache.org/jira/secure/attachment/12675504/Screen%20Shot%202014-10-16%20at%209.35.26%20PM.png
   * has more examples.
   * To report progress, we'll assume all tasks are equal size and compute "completed" as percent of "total"
   * "(Map|Reducer) (\\d+:) ((-/-)|(\\d+(\\(\\+\\d+\\))?/\\d+))" is the complete pattern but we'll drop "-/-" to exclude
   * groups that don't add information such as "Map 1: -/-"
   */
  public static final Pattern HIVE_TEZ_COMPLETE = Pattern.compile("(Map|Reducer) (\\d+:) (\\d+(\\(\\+\\d+\\))?/\\d+)");
  public static final Pattern HIVE_BEELINE_COMPLETE = Pattern.compile("VERTICES: .* (\\d+%)");
  /**
   * Pig on Tez produces progress report that looks like this
   * DAG Status: status=RUNNING, progress=TotalTasks: 3 Succeeded: 0 Running: 0 Failed: 0 Killed: 0
   *
   * Use Succeeded/TotalTasks to report progress
   * There is a hole as Pig might launch more than one DAGs. If this happens, user might
   * see progress rewind since the percentage is for the new DAG. To fix this, We need to fix
   * Pig print total number of DAGs on console, and track complete DAGs in WebHCat.
   */
  public static final Pattern PIG_TEZ_COMPLETE = Pattern.compile("progress=TotalTasks: (\\d+) Succeeded: (\\d+)");
  public static final Pattern TEZ_COUNTERS = Pattern.compile("\\d+");

  /**
   * Extract the percent complete line from Pig or Jar jobs.
   */
  public static String extractPercentComplete(String line) {
    Matcher jar = JAR_COMPLETE.matcher(line);
    if (jar.find())
      return jar.group().trim();

    Matcher pig = PIG_COMPLETE.matcher(line);
    if (pig.find())
      return pig.group().trim();

    Matcher beeline = HIVE_BEELINE_COMPLETE.matcher(line);
    if (beeline.find()) {
      return beeline.group(1).trim() + " complete";
    }

    Matcher hive = HIVE_COMPLETE.matcher(line);
    if(hive.find()) {
      return "map " + hive.group(1) + " reduce " + hive.group(2);
    }
    Matcher hiveTez = HIVE_TEZ_COMPLETE.matcher(line);
    if(hiveTez.find()) {
      int totalTasks = 0;
      int completedTasks = 0;
      do {
        //here each group looks something like "Map 2: 2/4" "Reducer 3: 1(+2)/4"
        //just parse the numbers and ignore one from "Map 2" and from "(+2)" if it's there
        Matcher counts = TEZ_COUNTERS.matcher(hiveTez.group());
        List<String> items = new ArrayList<String>(4);
        while(counts.find()) {
          items.add(counts.group());
        }
        completedTasks += Integer.parseInt(items.get(1));
        if(items.size() == 3) {
          totalTasks += Integer.parseInt(items.get(2));
        }
        else {
          totalTasks += Integer.parseInt(items.get(3));
        }
      } while(hiveTez.find());
      if(totalTasks == 0) {
        return "0% complete (0 total tasks)";
      }
      return completedTasks * 100 / totalTasks + "% complete";
    }
    Matcher pigTez = PIG_TEZ_COMPLETE.matcher(line);
    if(pigTez.find()) {
      int totalTasks = Integer.parseInt(pigTez.group(1));
      int completedTasks = Integer.parseInt(pigTez.group(2));
      if(totalTasks == 0) {
          return "0% complete (0 total tasks)";
        }
        return completedTasks * 100 / totalTasks + "% complete";
    }
    return null;
  }

  public static final Pattern JAR_ID = Pattern.compile(" Running job: (\\S+)$");
  public static final Pattern PIG_ID = Pattern.compile(" HadoopJobId: (\\S+)$");
  public static final Pattern[] ID_PATTERNS = {JAR_ID, PIG_ID};

  /**
   * Extract the job id from jar jobs.
   */
  public static String extractChildJobId(String line) {
    for (Pattern p : ID_PATTERNS) {
      Matcher m = p.matcher(line);
      if (m.find())
        return m.group(1);
    }

    return null;
  }

  /**
   * Take an array of strings and encode it into one string.
   */
  public static String encodeArray(String[] plain) {
    if (plain == null)
      return null;

    String[] escaped = new String[plain.length];

    for (int i = 0; i < plain.length; ++i) {
      if (plain[i] == null) {
        plain[i] = "";
      }
      escaped[i] = StringUtils.escapeString(plain[i]);
    }

    return StringUtils.arrayToString(escaped);
  }

  /**
   * Encode a List into a string.
   */
  public static String encodeArray(List<String> list) {
    if (list == null)
      return null;
    String[] array = new String[list.size()];
    return encodeArray(list.toArray(array));
  }

  /**
   * Take an encode strings and decode it into an array of strings.
   */
  public static String[] decodeArray(String s) {
    if (s == null)
      return null;

    String[] escaped = StringUtils.split(s);
    String[] plain = new String[escaped.length];

    for (int i = 0; i < escaped.length; ++i)
      plain[i] = StringUtils.unEscapeString(escaped[i]);

    return plain;
  }

  public static String[] hadoopFsListAsArray(String files, Configuration conf,
                         String user)
    throws URISyntaxException, FileNotFoundException, IOException,
    InterruptedException {
    if (files == null || conf == null) {
      return null;
    }
    String[] dirty = files.split(",");
    String[] clean = new String[dirty.length];

    for (int i = 0; i < dirty.length; ++i)
      clean[i] = hadoopFsFilename(dirty[i], conf, user);

    return clean;
  }

  public static String hadoopFsListAsString(String files, Configuration conf,
                        String user)
    throws URISyntaxException, FileNotFoundException, IOException,
    InterruptedException {
    if (files == null || conf == null) {
      return null;
    }
    return StringUtils.arrayToString(hadoopFsListAsArray(files, conf, user));
  }

  public static String hadoopFsFilename(String fname, Configuration conf, String user)
    throws URISyntaxException, FileNotFoundException, IOException,
    InterruptedException {
    Path p = hadoopFsPath(fname, conf, user);
    if (p == null)
      return null;
    else
      return p.toString();
  }

  /**
   * Returns all files (non-recursive) in {@code dirName}
   */
  public static List<Path> hadoopFsListChildren(String dirName, Configuration conf, String user)
    throws URISyntaxException, IOException, InterruptedException {

    Path p = hadoopFsPath(dirName, conf, user);
    FileSystem fs =  p.getFileSystem(conf);
    if(!fs.exists(p)) {
      return Collections.emptyList();
    }
    FileStatus[] children = fs.listStatus(p);
    if(!isset(children)) {
      return Collections.emptyList();
    }
    List<Path> files = new ArrayList<Path>();
    for(FileStatus stat : children) {
      files.add(stat.getPath());
    }
    return files;
  }

  /**
   * @return true iff we are sure the file is not there.
   */
  public static boolean hadoopFsIsMissing(FileSystem fs, Path p) {
    try {
      return !fs.exists(p);
    } catch (Throwable t) {
      // Got an error, might be there anyway due to a
      // permissions problem.
      return false;
    }
  }

  public static String addUserHomeDirectoryIfApplicable(String origPathStr, String user)
    throws IOException, URISyntaxException {
    if(origPathStr == null || origPathStr.isEmpty()) {
      return "/user/" + user;
    }
    Path p = new Path(origPathStr);
    if(p.isAbsolute()) {
      return origPathStr;
    }
    if(p.toUri().getPath().isEmpty()) {
      //origPathStr="hdfs://host:99" for example
      return new Path(p.toUri().getScheme(), p.toUri().getAuthority(), "/user/" + user).toString();
    }
    //can't have relative path if there is scheme/authority
    return "/user/" + user + "/" + origPathStr;
  }

  public static Path hadoopFsPath(String fname, final Configuration conf, String user)
    throws URISyntaxException, IOException, InterruptedException {
    if (fname == null || conf == null) {
      return null;
    }

    UserGroupInformation ugi;
    if (user!=null) {
      ugi = UgiFactory.getUgi(user);
    } else {
      ugi = UserGroupInformation.getLoginUser();
    }
    final String finalFName = new String(fname);

    final FileSystem defaultFs =
        ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
          @Override
          public FileSystem run()
            throws URISyntaxException, IOException, InterruptedException {
            return FileSystem.get(new URI(finalFName), conf);
          }
        });

    fname = addUserHomeDirectoryIfApplicable(fname, user);
    URI u = new URI(fname);
    Path p = new Path(u).makeQualified(defaultFs);

    if (hadoopFsIsMissing(defaultFs, p))
      throw new FileNotFoundException("File " + fname + " does not exist.");

    FileSystem.closeAllForUGI(ugi);
    return p;
  }

  /**
   * GET the given url.  Returns the number of bytes received.
   */
  public static int fetchUrl(URL url)
    throws IOException {
    URLConnection cnx = url.openConnection();
    InputStream in = cnx.getInputStream();

    byte[] buf = new byte[8192];
    int total = 0;
    int len = 0;
    while ((len = in.read(buf)) >= 0)
      total += len;

    return total;
  }

  /**
   * Set the environment variables to specify the hadoop user.
   */
  public static Map<String, String> hadoopUserEnv(String user,
                          String overrideClasspath) {
    HashMap<String, String> env = new HashMap<String, String>();
    env.put("HADOOP_USER_NAME", user);

    if (overrideClasspath != null) {
      env.put("HADOOP_USER_CLASSPATH_FIRST", "true");
      String cur = System.getenv("HADOOP_CLASSPATH");
      if (TempletonUtils.isset(cur))
        overrideClasspath = overrideClasspath + ":" + cur;
      env.put("HADOOP_CLASSPATH", overrideClasspath);
    }

    return env;
  }

  /**
   * replaces all occurrences of "\," with ","; returns {@code s} if no modifications needed
   */
  public static String unEscapeString(String s) {
    return s != null && s.contains("\\,") ? StringUtils.unEscapeString(s) : s;
  }

  /**
   * Find a jar that contains a class of the same name and which
   * file name matches the given pattern.
   *
   * @param clazz the class to find.
   * @param fileNamePattern regex pattern that must match the jar full path
   * @return a jar file that contains the class, or null
   */
  public static String findContainingJar(Class<?> clazz, String fileNamePattern) {
    ClassLoader loader = clazz.getClassLoader();
    String classFile = clazz.getName().replaceAll("\\.", "/") + ".class";
    try {
      for(final Enumeration<URL> itr = loader.getResources(classFile);
          itr.hasMoreElements();) {
        final URL url = itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (fileNamePattern == null || toReturn.matches(fileNamePattern)) {
            toReturn = URLDecoder.decode(toReturn, "UTF-8");
            return toReturn.replaceAll("!.*$", "");
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
  public static StringBuilder dumpPropMap(String header, Properties props) {
    Map<String, String> map = new HashMap<String, String>();
    for(Map.Entry<Object, Object> ent : props.entrySet()) {
      map.put(ent.getKey().toString(), ent.getValue() == null ? null : ent.getValue().toString());
    }
    return dumpPropMap(header, map);
  }
  public static StringBuilder dumpPropMap(String header, Map<String, String> map) {
    StringBuilder sb = new StringBuilder("START").append(header).append(":\n");
    List<String> propKeys = new ArrayList<String>(map.keySet());
    Collections.sort(propKeys);
    for(String propKey : propKeys) {
      if(propKey.toLowerCase().contains("path")) {
        StringTokenizer st = new StringTokenizer(map.get(propKey), File.pathSeparator);
        if(st.countTokens() > 1) {
          sb.append(propKey).append("=\n");
          while (st.hasMoreTokens()) {
            sb.append("    ").append(st.nextToken()).append(File.pathSeparator).append('\n');
          }
        }
        else {
          sb.append(propKey).append('=').append(map.get(propKey)).append('\n');
        }
      }
      else {
        sb.append(propKey).append('=').append(LogUtils.maskIfPassword(propKey, map.get(propKey)));
        sb.append('\n');
      }
    }
    return sb.append("END").append(header).append('\n');
  }
}
