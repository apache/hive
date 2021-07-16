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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * RangerPolicy class to contain Ranger Policy details.
 */
@JsonAutoDetect(fieldVisibility = Visibility.ANY)
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerPolicy extends RangerBaseModelObject implements java.io.Serializable {
  public static final int POLICY_TYPE_ACCESS = 0;
  public static final int POLICY_TYPE_DATAMASK = 1;
  public static final int POLICY_TYPE_ROWFILTER = 2;

  public static final int[] POLICY_TYPES = new int[]{
      POLICY_TYPE_ACCESS,
      POLICY_TYPE_DATAMASK,
      POLICY_TYPE_ROWFILTER,
  };

  public static final String MASK_TYPE_NULL = "MASK_NULL";
  public static final String MASK_TYPE_NONE = "MASK_NONE";
  public static final String MASK_TYPE_CUSTOM = "CUSTOM";

  private static final long serialVersionUID = 1L;

  private String service;
  private String name;
  private Integer policyType;
  private String description;
  private String resourceSignature;
  private Boolean isAuditEnabled;
  private Map<String, RangerPolicyResource> resources;
  private List<RangerPolicyItem> policyItems;
  private List<RangerPolicyItem> denyPolicyItems;
  private List<RangerPolicyItem> allowExceptions;
  private List<RangerPolicyItem> denyExceptions;
  private List<RangerDataMaskPolicyItem> dataMaskPolicyItems;
  private List<RangerRowFilterPolicyItem> rowFilterPolicyItems;


  /**
   * Ranger Policy default constructor.
   */
  public RangerPolicy() {
    this(null, null, null, null, null, null, null);
  }

  /**
   * @param service
   * @param name
   * @param policyType
   * @param description
   * @param resources
   * @param policyItems
   * @param resourceSignature TODO
   */
  public RangerPolicy(String service, String name, Integer policyType, String description,
                      Map<String, RangerPolicyResource> resources, List<RangerPolicyItem> policyItems,
                      String resourceSignature) {
    super();
    setService(service);
    setName(name);
    setPolicyType(policyType);
    setDescription(description);
    setResourceSignature(resourceSignature);
    setIsAuditEnabled(null);
    setResources(resources);
    setPolicyItems(policyItems);
    setDenyPolicyItems(null);
    setAllowExceptions(null);
    setDenyExceptions(null);
    setDataMaskPolicyItems(null);
    setRowFilterPolicyItems(null);
  }

  /**
   * @param other
   */
  public void updateFrom(RangerPolicy other) {
    super.updateFrom(other);

    setService(other.getService());
    setName(other.getName());
    setPolicyType(other.getPolicyType());
    setDescription(other.getDescription());
    setResourceSignature(other.getResourceSignature());
    setIsAuditEnabled(other.getIsAuditEnabled());
    setResources(other.getResources());
    setPolicyItems(other.getPolicyItems());
    setDenyPolicyItems(other.getDenyPolicyItems());
    setAllowExceptions(other.getAllowExceptions());
    setDenyExceptions(other.getDenyExceptions());
    setDataMaskPolicyItems(other.getDataMaskPolicyItems());
    setRowFilterPolicyItems(other.getRowFilterPolicyItems());
  }

  /**
   * @return the type
   */
  public String getService() {
    return service;
  }

  /**
   * @param service the type to set
   */
  public void setService(String service) {
    this.service = service;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the policyType
   */
  public Integer getPolicyType() {
    return policyType;
  }

  /**
   * @param policyType the policyType to set
   */
  public void setPolicyType(Integer policyType) {
    this.policyType = policyType;
  }

  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description the description to set
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * @return the resourceSignature
   */
  public String getResourceSignature() {
    return resourceSignature;
  }

  /**
   * @param resourceSignature the resourceSignature to set
   */
  public void setResourceSignature(String resourceSignature) {
    this.resourceSignature = resourceSignature;
  }

  /**
   * @return the isAuditEnabled
   */
  public Boolean getIsAuditEnabled() {
    return isAuditEnabled;
  }

  /**
   * @param isAuditEnabled the isEnabled to set
   */
  public void setIsAuditEnabled(Boolean isAuditEnabled) {
    this.isAuditEnabled = isAuditEnabled == null ? Boolean.TRUE : isAuditEnabled;
  }

  /**
   * @return the resources
   */
  public Map<String, RangerPolicyResource> getResources() {
    return resources;
  }

  /**
   * @param resources the resources to set
   */
  public void setResources(Map<String, RangerPolicyResource> resources) {
    if (this.resources == null) {
      this.resources = new HashMap<>();
    }

    if (this.resources == resources) {
      return;
    }

    this.resources.clear();

    if (resources != null) {
      for (Map.Entry<String, RangerPolicyResource> e : resources.entrySet()) {
        this.resources.put(e.getKey(), e.getValue());
      }
    }
  }

  /**
   * @return the policyItems
   */
  public List<RangerPolicyItem> getPolicyItems() {
    return policyItems;
  }

  /**
   * @param policyItems the policyItems to set
   */
  public void setPolicyItems(List<RangerPolicyItem> policyItems) {
    if (this.policyItems == null) {
      this.policyItems = new ArrayList<>();
    }

    if (this.policyItems == policyItems) {
      return;
    }

    this.policyItems.clear();

    if (policyItems != null) {
      this.policyItems.addAll(policyItems);
    }
  }

  /**
   * @return the denyPolicyItems
   */
  public List<RangerPolicyItem> getDenyPolicyItems() {
    return denyPolicyItems;
  }

  /**
   * @param denyPolicyItems the denyPolicyItems to set
   */
  public void setDenyPolicyItems(List<RangerPolicyItem> denyPolicyItems) {
    if (this.denyPolicyItems == null) {
      this.denyPolicyItems = new ArrayList<>();
    }

    if (this.denyPolicyItems == denyPolicyItems) {
      return;
    }

    this.denyPolicyItems.clear();

    if (denyPolicyItems != null) {
      this.denyPolicyItems.addAll(denyPolicyItems);
    }
  }

  /**
   * @return the allowExceptions
   */
  public List<RangerPolicyItem> getAllowExceptions() {
    return allowExceptions;
  }

  /**
   * @param allowExceptions the allowExceptions to set
   */
  public void setAllowExceptions(List<RangerPolicyItem> allowExceptions) {
    if (this.allowExceptions == null) {
      this.allowExceptions = new ArrayList<>();
    }

    if (this.allowExceptions == allowExceptions) {
      return;
    }

    this.allowExceptions.clear();

    if (allowExceptions != null) {
      this.allowExceptions.addAll(allowExceptions);
    }
  }

  /**
   * @return the denyExceptions
   */
  public List<RangerPolicyItem> getDenyExceptions() {
    return denyExceptions;
  }

  /**
   * @param denyExceptions the denyExceptions to set
   */
  public void setDenyExceptions(List<RangerPolicyItem> denyExceptions) {
    if (this.denyExceptions == null) {
      this.denyExceptions = new ArrayList<>();
    }

    if (this.denyExceptions == denyExceptions) {
      return;
    }

    this.denyExceptions.clear();

    if (denyExceptions != null) {
      this.denyExceptions.addAll(denyExceptions);
    }
  }

  public List<RangerDataMaskPolicyItem> getDataMaskPolicyItems() {
    return dataMaskPolicyItems;
  }

  public void setDataMaskPolicyItems(List<RangerDataMaskPolicyItem> dataMaskPolicyItems) {
    if (this.dataMaskPolicyItems == null) {
      this.dataMaskPolicyItems = new ArrayList<>();
    }

    if (this.dataMaskPolicyItems == dataMaskPolicyItems) {
      return;
    }

    this.dataMaskPolicyItems.clear();

    if (dataMaskPolicyItems != null) {
      this.dataMaskPolicyItems.addAll(dataMaskPolicyItems);
    }
  }

  public List<RangerRowFilterPolicyItem> getRowFilterPolicyItems() {
    return rowFilterPolicyItems;
  }

  public void setRowFilterPolicyItems(List<RangerRowFilterPolicyItem> rowFilterPolicyItems) {
    if (this.rowFilterPolicyItems == null) {
      this.rowFilterPolicyItems = new ArrayList<>();
    }

    if (this.rowFilterPolicyItems == rowFilterPolicyItems) {
      return;
    }

    this.rowFilterPolicyItems.clear();

    if (rowFilterPolicyItems != null) {
      this.rowFilterPolicyItems.addAll(rowFilterPolicyItems);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    toString(sb);
    return sb.toString();
  }

  public StringBuilder toString(StringBuilder sb) {
    sb.append("RangerPolicy={");

    super.toString(sb);

    sb.append("service={").append(service).append("} ");
    sb.append("name={").append(name).append("} ");
    sb.append("policyType={").append(policyType).append("} ");
    sb.append("description={").append(description).append("} ");
    sb.append("resourceSignature={").append(resourceSignature).append("} ");
    sb.append("isAuditEnabled={").append(isAuditEnabled).append("} ");

    sb.append("resources={");
    if (resources != null) {
      for (Map.Entry<String, RangerPolicyResource> e : resources.entrySet()) {
        sb.append(e.getKey()).append("={");
        e.getValue().toString(sb);
        sb.append("} ");
      }
    }
    sb.append("} ");

    sb.append("policyItems={");
    if (policyItems != null) {
      for (RangerPolicyItem policyItem : policyItems) {
        if (policyItem != null) {
          policyItem.toString(sb);
        }
      }
    }
    sb.append("} ");

    sb.append("denyPolicyItems={");
    if (denyPolicyItems != null) {
      for (RangerPolicyItem policyItem : denyPolicyItems) {
        if (policyItem != null) {
          policyItem.toString(sb);
        }
      }
    }
    sb.append("} ");

    sb.append("allowExceptions={");
    if (allowExceptions != null) {
      for (RangerPolicyItem policyItem : allowExceptions) {
        if (policyItem != null) {
          policyItem.toString(sb);
        }
      }
    }
    sb.append("} ");

    sb.append("denyExceptions={");
    if (denyExceptions != null) {
      for (RangerPolicyItem policyItem : denyExceptions) {
        if (policyItem != null) {
          policyItem.toString(sb);
        }
      }
    }
    sb.append("} ");

    sb.append("dataMaskPolicyItems={");
    if (dataMaskPolicyItems != null) {
      for (RangerDataMaskPolicyItem dataMaskPolicyItem : dataMaskPolicyItems) {
        if (dataMaskPolicyItem != null) {
          dataMaskPolicyItem.toString(sb);
        }
      }
    }
    sb.append("} ");

    sb.append("rowFilterPolicyItems={");
    if (rowFilterPolicyItems != null) {
      for (RangerRowFilterPolicyItem rowFilterPolicyItem : rowFilterPolicyItems) {
        if (rowFilterPolicyItem != null) {
          rowFilterPolicyItem.toString(sb);
        }
      }
    }
    sb.append("} ");

    sb.append("}");

    return sb;
  }

  /**
   * RangerPolicyResource class to store the resource path values.
   */
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @XmlRootElement
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class RangerPolicyResource implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private List<String> values;
    private Boolean isExcludes;
    private Boolean isRecursive;

    public RangerPolicyResource() {
      this((List<String>) null, null, null);
    }

    public RangerPolicyResource(String value) {
      setValue(value);
      setIsExcludes(null);
      setIsRecursive(null);
    }

    public RangerPolicyResource(String value, Boolean isExcludes, Boolean isRecursive) {
      setValue(value);
      setIsExcludes(isExcludes);
      setIsRecursive(isRecursive);
    }

    public RangerPolicyResource(List<String> values, Boolean isExcludes, Boolean isRecursive) {
      setValues(values);
      setIsExcludes(isExcludes);
      setIsRecursive(isRecursive);
    }

    /**
     * @return the values
     */
    public List<String> getValues() {
      return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(List<String> values) {
      if (this.values == null) {
        this.values = new ArrayList<>();
      }
      if (this.values == values) {
        return;
      }
      this.values.clear();
      if (values != null) {
        this.values.addAll(values);
      }
    }

    /**
     * @param value the value to set
     */
    public void setValue(String value) {
      if (this.values == null) {
        this.values = new ArrayList<>();
      }
      this.values.clear();
      this.values.add(value);
    }

    /**
     * @return the isExcludes
     */
    public Boolean getIsExcludes() {
      return isExcludes;
    }

    /**
     * @param isExcludes the isExcludes to set
     */
    public void setIsExcludes(Boolean isExcludes) {
      this.isExcludes = isExcludes == null ? Boolean.FALSE : isExcludes;
    }

    /**
     * @return the isRecursive
     */
    public Boolean getIsRecursive() {
      return isRecursive;
    }

    /**
     * @param isRecursive the isRecursive to set
     */
    public void setIsRecursive(Boolean isRecursive) {
      this.isRecursive = isRecursive == null ? Boolean.FALSE : isRecursive;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb);
      return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
      sb.append("RangerPolicyResource={");
      sb.append("values={");
      if (values != null) {
        for (String value : values) {
          sb.append(value).append(" ");
        }
      }
      sb.append("} ");
      sb.append("isExcludes={").append(isExcludes).append("} ");
      sb.append("isRecursive={").append(isRecursive).append("} ");
      sb.append("}");

      return sb;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((isExcludes == null) ? 0 : isExcludes.hashCode());
      result = prime * result
          + ((isRecursive == null) ? 0 : isRecursive.hashCode());
      result = prime * result
          + ((values == null) ? 0 : values.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      RangerPolicyResource other = (RangerPolicyResource) obj;
      if (isExcludes == null) {
        if (other.isExcludes != null) {
          return false;
        }
      } else if (!isExcludes.equals(other.isExcludes)) {
        return false;
      }
      if (isRecursive == null) {
        if (other.isRecursive != null) {
          return false;
        }
      } else if (!isRecursive.equals(other.isRecursive)) {
        return false;
      }
      if (values == null) {
        if (other.values != null) {
          return false;
        }
      } else if (!values.equals(other.values)) {
        return false;
      }
      return true;
    }
  }

  /**
   * RangerPolicyItem class contains ranger policy items like access and permissions.
   */
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @XmlRootElement
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class RangerPolicyItem implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private List<RangerPolicyItemAccess> accesses;
    private List<String> users;
    private List<String> groups;
    private List<RangerPolicyItemCondition> conditions;
    private Boolean delegateAdmin;

    public RangerPolicyItem() {
      this(null, null, null, null, null);
    }

    public RangerPolicyItem(List<RangerPolicyItemAccess> accessTypes, List<String> users, List<String> groups,
                            List<RangerPolicyItemCondition> conditions, Boolean delegateAdmin) {
      setAccesses(accessTypes);
      setUsers(users);
      setGroups(groups);
      setConditions(conditions);
      setDelegateAdmin(delegateAdmin);
    }

    /**
     * @return the accesses
     */
    public List<RangerPolicyItemAccess> getAccesses() {
      return accesses;
    }

    /**
     * @param accesses the accesses to set
     */
    public void setAccesses(List<RangerPolicyItemAccess> accesses) {
      if (this.accesses == null) {
        this.accesses = new ArrayList<>();
      }

      if (this.accesses == accesses) {
        return;
      }

      this.accesses.clear();

      if (accesses != null) {
        this.accesses.addAll(accesses);
      }
    }

    /**
     * @return the users
     */
    public List<String> getUsers() {
      return users;
    }

    /**
     * @param users the users to set
     */
    public void setUsers(List<String> users) {
      if (this.users == null) {
        this.users = new ArrayList<>();
      }

      if (this.users == users) {
        return;
      }

      this.users.clear();

      if (users != null) {
        this.users.addAll(users);
      }
    }

    /**
     * @return the groups
     */
    public List<String> getGroups() {
      return groups;
    }

    /**
     * @param groups the groups to set
     */
    public void setGroups(List<String> groups) {
      if (this.groups == null) {
        this.groups = new ArrayList<>();
      }
      if (this.groups == groups) {
        return;
      }
      this.groups.clear();
      if (groups != null) {
        this.groups.addAll(groups);
      }
    }

    /**
     * @return the conditions
     */
    public List<RangerPolicyItemCondition> getConditions() {
      return conditions;
    }

    /**
     * @param conditions the conditions to set
     */
    public void setConditions(List<RangerPolicyItemCondition> conditions) {
      if (this.conditions == null) {
        this.conditions = new ArrayList<>();
      }
      if (this.conditions == conditions) {
        return;
      }
      this.conditions.clear();
      if (conditions != null) {
        this.conditions.addAll(conditions);
      }
    }

    /**
     * @return the delegateAdmin
     */
    public Boolean getDelegateAdmin() {
      return delegateAdmin;
    }

    /**
     * @param delegateAdmin the delegateAdmin to set
     */
    public void setDelegateAdmin(Boolean delegateAdmin) {
      this.delegateAdmin = delegateAdmin == null ? Boolean.FALSE : delegateAdmin;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb);
      return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
      sb.append("RangerPolicyItem={");
      sb.append("accessTypes={");
      if (accesses != null) {
        for (RangerPolicyItemAccess access : accesses) {
          if (access != null) {
            access.toString(sb);
          }
        }
      }
      sb.append("} ");
      sb.append("users={");
      if (users != null) {
        for (String user : users) {
          if (user != null) {
            sb.append(user).append(" ");
          }
        }
      }
      sb.append("} ");
      sb.append("groups={");
      if (groups != null) {
        for (String group : groups) {
          if (group != null) {
            sb.append(group).append(" ");
          }
        }
      }
      sb.append("} ");
      sb.append("conditions={");
      if (conditions != null) {
        for (RangerPolicyItemCondition condition : conditions) {
          if (condition != null) {
            condition.toString(sb);
          }
        }
      }
      sb.append("} ");
      sb.append("delegateAdmin={").append(delegateAdmin).append("} ");
      sb.append("}");

      return sb;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((accesses == null) ? 0 : accesses.hashCode());
      result = prime * result
          + ((conditions == null) ? 0 : conditions.hashCode());
      result = prime * result
          + ((delegateAdmin == null) ? 0 : delegateAdmin.hashCode());
      result = prime * result
          + ((groups == null) ? 0 : groups.hashCode());
      result = prime * result + ((users == null) ? 0 : users.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      RangerPolicyItem other = (RangerPolicyItem) obj;
      if (accesses == null) {
        if (other.accesses != null) {
          return false;
        }
      } else if (!accesses.equals(other.accesses)) {
        return false;
      }
      if (conditions == null) {
        if (other.conditions != null) {
          return false;
        }
      } else if (!conditions.equals(other.conditions)) {
        return false;
      }
      if (delegateAdmin == null) {
        if (other.delegateAdmin != null) {
          return false;
        }
      } else if (!delegateAdmin.equals(other.delegateAdmin)) {
        return false;
      }
      if (groups == null) {
        if (other.groups != null) {
          return false;
        }
      } else if (!groups.equals(other.groups)) {
        return false;
      }
      if (users == null) {
        if (other.users != null) {
          return false;
        }
      } else if (!users.equals(other.users)) {
        return false;
      }
      return true;

    }
  }

  /**
   * RangerDataMaskPolicyItem class.
   */
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @XmlRootElement
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class RangerDataMaskPolicyItem extends RangerPolicyItem implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private RangerPolicyItemDataMaskInfo dataMaskInfo;

    public RangerDataMaskPolicyItem() {
      this(null, null, null, null, null, null);
    }

    public RangerDataMaskPolicyItem(List<RangerPolicyItemAccess> accesses,
                                    RangerPolicyItemDataMaskInfo dataMaskDetail, List<String> users,
                                    List<String> groups,
                                    List<RangerPolicyItemCondition> conditions, Boolean delegateAdmin) {
      super(accesses, users, groups, conditions, delegateAdmin);
      setDataMaskInfo(dataMaskDetail);
    }

    /**
     * @return the dataMaskInfo
     */
    public RangerPolicyItemDataMaskInfo getDataMaskInfo() {
      return dataMaskInfo;
    }

    /**
     * @param dataMaskInfo the dataMaskInfo to set
     */
    public void setDataMaskInfo(RangerPolicyItemDataMaskInfo dataMaskInfo) {
      this.dataMaskInfo = dataMaskInfo == null ? new RangerPolicyItemDataMaskInfo() : dataMaskInfo;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((dataMaskInfo == null) ? 0 : dataMaskInfo.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      RangerDataMaskPolicyItem other = (RangerDataMaskPolicyItem) obj;
      if (dataMaskInfo == null) {
        if (other.dataMaskInfo != null) {
          return false;
        }
      } else if (!dataMaskInfo.equals(other.dataMaskInfo)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb);
      return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
      sb.append("RangerDataMaskPolicyItem={");
      super.toString(sb);
      sb.append("dataMaskInfo={");
      if (dataMaskInfo != null) {
        dataMaskInfo.toString(sb);
      }
      sb.append("} ");
      sb.append("}");
      return sb;
    }
  }

  /**
   * RangerRowFilterPolicyItem class.
   */
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @XmlRootElement
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class RangerRowFilterPolicyItem extends RangerPolicyItem implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private RangerPolicyItemRowFilterInfo rowFilterInfo;

    public RangerRowFilterPolicyItem() {
      this(null, null, null, null, null, null);
    }

    public RangerRowFilterPolicyItem(RangerPolicyItemRowFilterInfo rowFilterInfo,
                                     List<RangerPolicyItemAccess> accesses, List<String> users, List<String> groups,
                                     List<RangerPolicyItemCondition> conditions, Boolean delegateAdmin) {
      super(accesses, users, groups, conditions, delegateAdmin);
      setRowFilterInfo(rowFilterInfo);
    }

    /**
     * @return the rowFilterInfo
     */
    public RangerPolicyItemRowFilterInfo getRowFilterInfo() {
      return rowFilterInfo;
    }

    /**
     * @param rowFilterInfo the rowFilterInfo to set
     */
    public void setRowFilterInfo(RangerPolicyItemRowFilterInfo rowFilterInfo) {
      this.rowFilterInfo = rowFilterInfo == null ? new RangerPolicyItemRowFilterInfo() : rowFilterInfo;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((rowFilterInfo == null) ? 0 : rowFilterInfo.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      RangerRowFilterPolicyItem other = (RangerRowFilterPolicyItem) obj;
      if (rowFilterInfo == null) {
        if (other.rowFilterInfo != null) {
          return false;
        }
      } else if (!rowFilterInfo.equals(other.rowFilterInfo)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb);
      return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
      sb.append("RangerRowFilterPolicyItem={");
      super.toString(sb);
      sb.append("rowFilterInfo={");
      if (rowFilterInfo != null) {
        rowFilterInfo.toString(sb);
      }
      sb.append("} ");
      sb.append("}");
      return sb;
    }
  }

  /**
   * RangerPolicyItemAccess class.
   */
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @XmlRootElement
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class RangerPolicyItemAccess implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String type;
    private Boolean isAllowed;

    public RangerPolicyItemAccess() {
      this(null, null);
    }

    public RangerPolicyItemAccess(String type) {
      this(type, null);
    }

    public RangerPolicyItemAccess(String type, Boolean isAllowed) {
      setType(type);
      setIsAllowed(isAllowed);
    }

    /**
     * @return the type
     */
    public String getType() {
      return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
      this.type = type;
    }

    /**
     * @return the isAllowed
     */
    public Boolean getIsAllowed() {
      return isAllowed;
    }

    /**
     * @param isAllowed the isAllowed to set
     */
    public void setIsAllowed(Boolean isAllowed) {
      this.isAllowed = isAllowed == null ? Boolean.TRUE : isAllowed;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb);
      return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
      sb.append("RangerPolicyItemAccess={");
      sb.append("type={").append(type).append("} ");
      sb.append("isAllowed={").append(isAllowed).append("} ");
      sb.append("}");
      return sb;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((isAllowed == null) ? 0 : isAllowed.hashCode());
      result = prime * result + ((type == null) ? 0 : type.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      RangerPolicyItemAccess other = (RangerPolicyItemAccess) obj;
      if (isAllowed == null) {
        if (other.isAllowed != null) {
          return false;
        }
      } else if (!isAllowed.equals(other.isAllowed)) {
        return false;
      }
      if (type == null) {
        if (other.type != null) {
          return false;
        }
      } else if (!type.equals(other.type)) {
        return false;
      }
      return true;
    }
  }

  /**
   * RangerPolicyItemCondition class to store policy conditions.
   */
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @XmlRootElement
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class RangerPolicyItemCondition implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String type;
    private List<String> values;

    public RangerPolicyItemCondition() {
      this(null, null);
    }

    public RangerPolicyItemCondition(String type, List<String> values) {
      setType(type);
      setValues(values);
    }

    /**
     * @return the type
     */
    public String getType() {
      return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
      this.type = type;
    }

    /**
     * @return the value
     */
    public List<String> getValues() {
      return values;
    }

    /**
     * @param values the value to set
     */
    public void setValues(List<String> values) {
      if (this.values == null) {
        this.values = new ArrayList<>();
      }
      if (this.values == values) {
        return;
      }
      this.values.clear();
      if (values != null) {
        this.values.addAll(values);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb);
      return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
      sb.append("RangerPolicyItemCondition={");
      sb.append("type={").append(type).append("} ");
      sb.append("values={");
      if (values != null) {
        for (String value : values) {
          sb.append(value).append(" ");
        }
      }
      sb.append("} ");
      sb.append("}");

      return sb;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((type == null) ? 0 : type.hashCode());
      result = prime * result + ((values == null) ? 0 : values.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      RangerPolicyItemCondition other = (RangerPolicyItemCondition) obj;
      if (type == null) {
        if (other.type != null) {
          return false;
        }
      } else if (!type.equals(other.type)) {
        return false;
      }
      if (values == null) {
        if (other.values != null) {
          return false;
        }
      } else if (!values.equals(other.values)) {
        return false;
      }
      return true;
    }
  }

  /**
   * RangerPolicyItemDataMaskInfo store policy having datamasking.
   */
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @XmlRootElement
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class RangerPolicyItemDataMaskInfo implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String dataMaskType;
    private String conditionExpr;
    private String valueExpr;

    public RangerPolicyItemDataMaskInfo() {
    }

    public RangerPolicyItemDataMaskInfo(String dataMaskType, String conditionExpr, String valueExpr) {
      setDataMaskType(dataMaskType);
      setConditionExpr(conditionExpr);
      setValueExpr(valueExpr);
    }

    public String getDataMaskType() {
      return dataMaskType;
    }

    public void setDataMaskType(String dataMaskType) {
      this.dataMaskType = dataMaskType;
    }

    public String getConditionExpr() {
      return conditionExpr;
    }

    public void setConditionExpr(String conditionExpr) {
      this.conditionExpr = conditionExpr;
    }

    public String getValueExpr() {
      return valueExpr;
    }

    public void setValueExpr(String valueExpr) {
      this.valueExpr = valueExpr;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((dataMaskType == null) ? 0 : dataMaskType.hashCode());
      result = prime * result + ((conditionExpr == null) ? 0 : conditionExpr.hashCode());
      result = prime * result + ((valueExpr == null) ? 0 : valueExpr.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      RangerPolicyItemDataMaskInfo other = (RangerPolicyItemDataMaskInfo) obj;
      if (dataMaskType == null) {
        if (other.dataMaskType != null) {
          return false;
        }
      } else if (!dataMaskType.equals(other.dataMaskType)) {
        return false;
      }
      if (conditionExpr == null) {
        if (other.conditionExpr != null) {
          return false;
        }
      } else if (!conditionExpr.equals(other.conditionExpr)) {
        return false;
      }
      if (valueExpr == null) {
        if (other.valueExpr != null) {
          return false;
        }
      } else if (!valueExpr.equals(other.valueExpr)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb);
      return sb.toString();
    }

    private StringBuilder toString(StringBuilder sb) {
      sb.append("RangerPolicyItemDataMaskInfo={");
      sb.append("dataMaskType={").append(dataMaskType).append("} ");
      sb.append("conditionExpr={").append(conditionExpr).append("} ");
      sb.append("valueExpr={").append(valueExpr).append("} ");
      sb.append("}");
      return sb;
    }
  }

  /**
   * Ranger policyItem Row-filter info class.
   */
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @XmlRootElement
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class RangerPolicyItemRowFilterInfo implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String filterExpr;

    public RangerPolicyItemRowFilterInfo() {
    }

    public RangerPolicyItemRowFilterInfo(String filterExpr) {
      setFilterExpr(filterExpr);
    }

    public String getFilterExpr() {
      return filterExpr;
    }

    public void setFilterExpr(String filterExpr) {
      this.filterExpr = filterExpr;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((filterExpr == null) ? 0 : filterExpr.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      RangerPolicyItemRowFilterInfo other = (RangerPolicyItemRowFilterInfo) obj;
      if (filterExpr == null) {
        if (other.filterExpr != null) {
          return false;
        }
      } else if (!filterExpr.equals(other.filterExpr)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb);
      return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
      sb.append("RangerPolicyItemDataMaskInfo={");
      sb.append("filterExpr={").append(filterExpr).append("} ");
      sb.append("}");
      return sb;
    }
  }
}
