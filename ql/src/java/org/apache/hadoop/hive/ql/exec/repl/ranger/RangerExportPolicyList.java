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

package org.apache.hadoop.hive.ql.exec.repl.ranger;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * RangerExportPolicyList class to extends RangerPolicyList class.
 */
@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE,
    fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerExportPolicyList extends RangerPolicyList implements java.io.Serializable {
  private static final long serialVersionUID = 1L;

  private Map<String, Object> metaDataInfo = new LinkedHashMap<String, Object>();

  public Map<String, Object> getMetaDataInfo() {
    return metaDataInfo;
  }

  public void setMetaDataInfo(Map<String, Object> metaDataInfo) {
    this.metaDataInfo = metaDataInfo;
  }

}
