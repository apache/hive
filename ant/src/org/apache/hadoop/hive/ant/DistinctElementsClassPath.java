/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.hadoop.hive.ant;


import java.io.File;
import java.util.ArrayList;
import java.util.HashSet; 

import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.Path;

/**
 * This object represents a path as used by CLASSPATH or PATH environment variable. String 
 * representation of this object returns the path with unique elements to reduce the chances of
 * exceeding  the character limit problem on windows by removing if there are duplicate files(JARs)
 * in the original class path.
 */
public class DistinctElementsClassPath extends Path {
  
  /**
   * Invoked by IntrospectionHelper for <code>setXXX(Path p)</code>
   * attribute setters.
   * @param p the <code>Project</code> for this path.
   * @param path the <code>String</code> path definition.
   */
  public DistinctElementsClassPath(Project p, String path) {
      super(p, path);
  }

  /**
   * Construct an empty <code>Path</code>.
   * @param project the <code>Project</code> for this path.
   */
  public DistinctElementsClassPath(Project project) {
    super(project);
  }

  /**
   * Returns the list of path elements after removing the duplicate files from the
   * original Path
   */
  @Override
  public String[] list() {
    HashSet includedElements = new HashSet();
    ArrayList resultElements = new ArrayList();
    for(String pathElement : super.list()) {
      if(pathElement != null && !pathElement.isEmpty()) {
        File p = new File(pathElement);
        if(p.exists()) {
          String setItem = pathElement.toLowerCase();
          if (p.isFile()) {
            setItem = p.getName().toLowerCase();
          }
          if(!includedElements.contains(setItem)) {
            includedElements.add(setItem);
            resultElements.add(pathElement);
          }
        }
      }
    }
    
    return (String[])resultElements.toArray (new String [resultElements.size ()]);
  }

  /**
   * Returns a textual representation of the path after removing the duplicate files from the
   * original Path.
   */
  @Override
  public String toString() {
    return org.apache.commons.lang.StringUtils.join(this.list(), File.pathSeparatorChar);
  }
}