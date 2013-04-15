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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.templeton.tool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

/**
 * General utility methods.
 */
public class TempletonUtils {
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


    public static final Pattern JAR_COMPLETE
        = Pattern.compile(" map \\d+%\\s+reduce \\d+%$");
    public static final Pattern PIG_COMPLETE = Pattern.compile(" \\d+% complete$");

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

    public static Path hadoopFsPath(String fname, Configuration conf, String user)
        throws URISyntaxException, FileNotFoundException, IOException,
        InterruptedException {
        if (fname == null || conf == null) {
            return null;
        }

        final Configuration fConf = new Configuration(conf);
        final String finalFName = new String(fname);

        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        final FileSystem defaultFs = 
                ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                    public FileSystem run() 
                        throws URISyntaxException, FileNotFoundException, IOException,
                            InterruptedException {
                        return FileSystem.get(new URI(finalFName), fConf);
                    }
                });

        URI u = new URI(fname);
        Path p = new Path(u).makeQualified(defaultFs);

        if (hadoopFsIsMissing(defaultFs, p))
            throw new FileNotFoundException("File " + fname + " does not exist.");

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
}
