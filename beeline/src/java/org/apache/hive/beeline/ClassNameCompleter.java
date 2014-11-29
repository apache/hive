/**
 * Copyright (c) 2002-2006, Marc Prud'hommeaux <mwp1@cornell.edu>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with
 * the distribution.
 *
 * Neither the name of JLine nor the names of its contributors
 * may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
 * OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
package org.apache.hive.beeline;

import jline.console.completer.StringsCompleter;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.Enumeration;
import java.util.TreeSet;

/**
 * the completer is original provided in JLine 0.9.94 and is being removed in 2.12. Add the
 * previous implement for usage of the beeline.
 */
public class ClassNameCompleter extends StringsCompleter {

  public ClassNameCompleter(String... candidates) {
    super(candidates);
  }

  public static String[] getClassNames() throws IOException {
    Set urls = new HashSet();

    for (ClassLoader loader = ClassNameCompleter.class.getClassLoader(); loader != null;
         loader = loader.getParent()) {
      if (!(loader instanceof URLClassLoader)) {
        continue;
      }

      urls.addAll(Arrays.asList(((URLClassLoader) loader).getURLs()));
    }

    // Now add the URL that holds java.lang.String. This is because
    // some JVMs do not report the core classes jar in the list of
    // class loaders.
    Class[] systemClasses = new Class[]{String.class, javax.swing.JFrame.class};

    for (int i = 0; i < systemClasses.length; i++) {
      URL classURL = systemClasses[i]
              .getResource("/" + systemClasses[i].getName().replace('.', '/') + ".class");

      if (classURL != null) {
        URLConnection uc = classURL.openConnection();

        if (uc instanceof JarURLConnection) {
          urls.add(((JarURLConnection) uc).getJarFileURL());
        }
      }
    }

    Set classes = new HashSet();

    for (Iterator i = urls.iterator(); i.hasNext(); ) {
      URL url = (URL) i.next();
      File file = new File(url.getFile());

      if (file.isDirectory()) {
        Set files = getClassFiles(file.getAbsolutePath(), new HashSet(), file, new int[]{200});
        classes.addAll(files);

        continue;
      }

      if ((file == null) || !file.isFile()) {
        continue;
      }

      JarFile jf = new JarFile(file);

      for (Enumeration e = jf.entries(); e.hasMoreElements(); ) {
        JarEntry entry = (JarEntry) e.nextElement();

        if (entry == null) {
          continue;
        }

        String name = entry.getName();

        if (!name.endsWith(".class")) {
          /* only use class file*/
          continue;
        }

        classes.add(name);
      }
    }

    // now filter classes by changing "/" to "." and trimming the
    // trailing ".class"
    Set classNames = new TreeSet();

    for (Iterator i = classes.iterator(); i.hasNext(); ) {
      String name = (String) i.next();
      classNames.add(name.replace('/', '.').
              substring(0, name.length() - 6));
    }

    return (String[]) classNames.toArray(new String[classNames.size()]);
  }

  private static Set getClassFiles(String root, Set holder, File directory, int[] maxDirectories) {
    // we have passed the maximum number of directories to scan
    if (maxDirectories[0]-- < 0) {
      return holder;
    }

    File[] files = directory.listFiles();

    for (int i = 0; (files != null) && (i < files.length); i++) {
      String name = files[i].getAbsolutePath();

      if (!(name.startsWith(root))) {
        continue;
      } else if (files[i].isDirectory()) {
        getClassFiles(root, holder, files[i], maxDirectories);
      } else if (files[i].getName().endsWith(".class")) {
        holder.add(files[i].getAbsolutePath().
                substring(root.length() + 1));
      }
    }

    return holder;
  }
}
