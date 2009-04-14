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

package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class UDFLike extends UDF {

  private static Log LOG = LogFactory.getLog(UDFLike.class.getName());
  private Text lastLikePattern = new Text();
  private Pattern p = null;

  private BooleanWritable result = new BooleanWritable();
  public UDFLike() {
  }

  public static String likePatternToRegExp(String likePattern) {
    StringBuilder sb = new StringBuilder();
    for(int i=0; i<likePattern.length(); i++) {
      // Make a special case for "\\_" and "\\%"
      char n = likePattern.charAt(i);
      if (n == '\\' && i+1 < likePattern.length() 
          && (likePattern.charAt(i+1) == '_' || likePattern.charAt(i+1) == '%')) {
        sb.append(likePattern.charAt(i+1));
        i++;
        continue;
      }

      if (n == '_') {
        sb.append(".");
      } else if (n == '%') {
        sb.append(".*");
      } else {
        if ("\\[](){}.*^$".indexOf(n) != -1) {
          sb.append('\\');
        }
        sb.append(n);
      }
    }
    return sb.toString();
  }
  
  public BooleanWritable evaluate(Text s, Text likePattern) {
    if (s == null || likePattern == null) {
      return null;
    }
    if (!likePattern.equals(lastLikePattern)) {
      lastLikePattern.set(likePattern);
      p = Pattern.compile(likePatternToRegExp(likePattern.toString()));
    }
    Matcher m = p.matcher(s.toString());
    result.set(m.matches());
    return result;
  }
  
}
