/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hive.accumulo;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Accumulo doesn't have a TableMapReduceUtil.addDependencyJars method like HBase which is very
 * helpful
 */
public class Utils {
  private static final Logger log = Logger.getLogger(Utils.class);

  // Thanks, HBase
  public static void addDependencyJars(Configuration conf, Class<?>... classes) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Set<String> jars = new HashSet<String>();
    // Add jars that are already in the tmpjars variable
    jars.addAll(conf.getStringCollection("tmpjars"));

    // add jars as we find them to a map of contents jar name so that we can
    // avoid
    // creating new jars for classes that have already been packaged.
    Map<String,String> packagedClasses = new HashMap<String,String>();

    // Add jars containing the specified classes
    for (Class<?> clazz : classes) {
      if (clazz == null)
        continue;

      Path path = findOrCreateJar(clazz, localFs, packagedClasses);
      if (path == null) {
        log.warn("Could not find jar for class " + clazz + " in order to ship it to the cluster.");
        continue;
      }
      if (!localFs.exists(path)) {
        log.warn("Could not validate jar file " + path + " for class " + clazz);
        continue;
      }
      jars.add(path.toString());
    }
    if (jars.isEmpty())
      return;

    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
  }

  /**
   * If org.apache.hadoop.util.JarFinder is available (0.23+ hadoop), finds the Jar for a class or
   * creates it if it doesn't exist. If the class is in a directory in the classpath, it creates a
   * Jar on the fly with the contents of the directory and returns the path to that Jar. If a Jar is
   * created, it is created in the system temporary directory. Otherwise, returns an existing jar
   * that contains a class of the same name. Maintains a mapping from jar contents to the tmp jar
   * created.
   *
   * @param my_class
   *          the class to find.
   * @param fs
   *          the FileSystem with which to qualify the returned path.
   * @param packagedClasses
   *          a map of class name to path.
   * @return a jar file that contains the class.
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  private static Path findOrCreateJar(Class<?> my_class, FileSystem fs,
      Map<String,String> packagedClasses) throws IOException {
    // attempt to locate an existing jar for the class.
    String jar = findContainingJar(my_class, packagedClasses);
    if (null == jar || jar.isEmpty()) {
      jar = getJar(my_class);
      updateMap(jar, packagedClasses);
    }

    if (null == jar || jar.isEmpty()) {
      return null;
    }

    log.debug(String.format("For class %s, using jar %s", my_class.getName(), jar));
    return new Path(jar).makeQualified(fs);
  }

  /**
   * Add entries to <code>packagedClasses</code> corresponding to class files contained in
   * <code>jar</code>.
   *
   * @param jar
   *          The jar who's content to list.
   * @param packagedClasses
   *          map[class -> jar]
   */
  private static void updateMap(String jar, Map<String,String> packagedClasses) throws IOException {
    if (null == jar || jar.isEmpty()) {
      return;
    }
    ZipFile zip = null;
    try {
      zip = new ZipFile(jar);
      for (Enumeration<? extends ZipEntry> iter = zip.entries(); iter.hasMoreElements();) {
        ZipEntry entry = iter.nextElement();
        if (entry.getName().endsWith("class")) {
          packagedClasses.put(entry.getName(), jar);
        }
      }
    } finally {
      if (null != zip)
        zip.close();
    }
  }

  /**
   * Find a jar that contains a class of the same name, if any. It will return a jar file, even if
   * that is not the first thing on the class path that has a class with the same name. Looks first
   * on the classpath and then in the <code>packagedClasses</code> map.
   *
   * @param my_class
   *          the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException
   */
  private static String findContainingJar(Class<?> my_class, Map<String,String> packagedClasses)
      throws IOException {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";

    // first search the classpath
    for (Enumeration<URL> itr = loader.getResources(class_file); itr.hasMoreElements();) {
      URL url = itr.nextElement();
      if ("jar".equals(url.getProtocol())) {
        String toReturn = url.getPath();
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length());
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        return toReturn.replaceAll("!.*$", "");
      }
    }

    // now look in any jars we've packaged using JarFinder. Returns null
    // when
    // no jar is found.
    return packagedClasses.get(class_file);
  }

  /**
   * Invoke 'getJar' on a JarFinder implementation. Useful for some job configuration contexts
   * (HBASE-8140) and also for testing on MRv2. First check if we have HADOOP-9426. Lacking that,
   * fall back to the backport.
   *
   * @param my_class
   *          the class to find.
   * @return a jar file that contains the class, or null.
   */
  private static String getJar(Class<?> my_class) {
    String ret = null;
    String hadoopJarFinder = "org.apache.hadoop.util.JarFinder";
    Class<?> jarFinder = null;
    try {
      log.debug("Looking for " + hadoopJarFinder + ".");
      jarFinder = JavaUtils.loadClass(hadoopJarFinder);
      log.debug(hadoopJarFinder + " found.");
      Method getJar = jarFinder.getMethod("getJar", Class.class);
      ret = (String) getJar.invoke(null, my_class);
    } catch (ClassNotFoundException e) {
      log.debug("Using backported JarFinder.");
      ret = jarFinderGetJar(my_class);
    } catch (InvocationTargetException e) {
      // function was properly called, but threw it's own exception.
      // Unwrap it
      // and pass it on.
      throw new RuntimeException(e.getCause());
    } catch (Exception e) {
      // toss all other exceptions, related to reflection failure
      throw new RuntimeException("getJar invocation failed.", e);
    }

    return ret;
  }

  /**
   * Returns the full path to the Jar containing the class. It always return a JAR.
   *
   * @param klass
   *          class.
   *
   * @return path to the Jar containing the class.
   */
  @SuppressWarnings("rawtypes")
  public static String jarFinderGetJar(Class klass) {
    Preconditions.checkNotNull(klass, "klass");
    ClassLoader loader = klass.getClassLoader();
    if (loader != null) {
      String class_file = klass.getName().replaceAll("\\.", "/") + ".class";
      try {
        for (Enumeration itr = loader.getResources(class_file); itr.hasMoreElements();) {
          URL url = (URL) itr.nextElement();
          String path = url.getPath();
          if (path.startsWith("file:")) {
            path = path.substring("file:".length());
          }
          path = URLDecoder.decode(path, "UTF-8");
          if ("jar".equals(url.getProtocol())) {
            path = URLDecoder.decode(path, "UTF-8");
            return path.replaceAll("!.*$", "");
          } else if ("file".equals(url.getProtocol())) {
            String klassName = klass.getName();
            klassName = klassName.replace(".", "/") + ".class";
            path = path.substring(0, path.length() - klassName.length());
            File baseDir = new File(path);
            File testDir = new File(System.getProperty("test.build.dir", "target/test-dir"));
            testDir = testDir.getAbsoluteFile();
            if (!testDir.exists()) {
              testDir.mkdirs();
            }
            File tempJar = File.createTempFile("hadoop-", "", testDir);
            tempJar = new File(tempJar.getAbsolutePath() + ".jar");
            createJar(baseDir, tempJar);
            return tempJar.getAbsolutePath();
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  private static void copyToZipStream(InputStream is, ZipEntry entry, ZipOutputStream zos)
      throws IOException {
    zos.putNextEntry(entry);
    byte[] arr = new byte[4096];
    int read = is.read(arr);
    while (read > -1) {
      zos.write(arr, 0, read);
      read = is.read(arr);
    }
    is.close();
    zos.closeEntry();
  }

  public static void jarDir(File dir, String relativePath, ZipOutputStream zos) throws IOException {
    Preconditions.checkNotNull(relativePath, "relativePath");
    Preconditions.checkNotNull(zos, "zos");

    // by JAR spec, if there is a manifest, it must be the first entry in
    // the
    // ZIP.
    File manifestFile = new File(dir, JarFile.MANIFEST_NAME);
    ZipEntry manifestEntry = new ZipEntry(JarFile.MANIFEST_NAME);
    if (!manifestFile.exists()) {
      zos.putNextEntry(manifestEntry);
      new Manifest().write(new BufferedOutputStream(zos));
      zos.closeEntry();
    } else {
      InputStream is = new FileInputStream(manifestFile);
      copyToZipStream(is, manifestEntry, zos);
    }
    zos.closeEntry();
    zipDir(dir, relativePath, zos, true);
    zos.close();
  }

  private static void zipDir(File dir, String relativePath, ZipOutputStream zos, boolean start)
      throws IOException {
    String[] dirList = dir.list();
    for (String aDirList : dirList) {
      File f = new File(dir, aDirList);
      if (!f.isHidden()) {
        if (f.isDirectory()) {
          if (!start) {
            ZipEntry dirEntry = new ZipEntry(relativePath + f.getName() + "/");
            zos.putNextEntry(dirEntry);
            zos.closeEntry();
          }
          String filePath = f.getPath();
          File file = new File(filePath);
          zipDir(file, relativePath + f.getName() + "/", zos, false);
        } else {
          String path = relativePath + f.getName();
          if (!path.equals(JarFile.MANIFEST_NAME)) {
            ZipEntry anEntry = new ZipEntry(path);
            InputStream is = new FileInputStream(f);
            copyToZipStream(is, anEntry, zos);
          }
        }
      }
    }
  }

  private static void createJar(File dir, File jarFile) throws IOException {
    Preconditions.checkNotNull(dir, "dir");
    Preconditions.checkNotNull(jarFile, "jarFile");
    File jarDir = jarFile.getParentFile();
    if (!jarDir.exists()) {
      if (!jarDir.mkdirs()) {
        throw new IOException(MessageFormat.format("could not create dir [{0}]", jarDir));
      }
    }
    JarOutputStream zos = new JarOutputStream(new FileOutputStream(jarFile));
    jarDir(dir, "", zos);
  }
}
