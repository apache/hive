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

package org.apache.hadoop.hive.metastore.model;

public class MDCPrivilege {
    private String principalName;

    private String principalType;

    private MDataConnector dataConnector;

    private int createTime;

    private String privilege;

    private String grantor;

    private String grantorType;

    private boolean grantOption;

    private String authorizer;

    public MDCPrivilege() {
    }

    public MDCPrivilege(String principalName, String principalType,
                        MDataConnector dataConnector, String dcPrivileges, int createTime, String grantor,
                        String grantorType, boolean grantOption, String authorizer) {
        super();
        this.principalName = principalName;
        this.principalType = principalType;
        this.dataConnector = dataConnector;
        this.privilege = dcPrivileges;
        this.createTime = createTime;
        this.grantorType = grantorType;
        this.grantOption = grantOption;
        this.grantor = grantor;
        this.authorizer = authorizer;
    }

    /**
     * @return user name, role name, or group name
     */
    public String getPrincipalName() {
        return principalName;
    }

    /**
     * @param userName user/role/group name
     */
    public void setPrincipalName(String userName) {
        this.principalName = userName;
    }

    /**
     * @return a set of privileges this user/role/group has
     */
    public String getPrivilege() {
        return privilege;
    }

    /**
     * @param dcPrivilege a set of privileges this user/role/group has
     */
    public void setPrivilege(String dcPrivilege) {
        this.privilege = dcPrivilege;
    }

    public MDataConnector getDataConnector() {
        return dataConnector;
    }

    public void setDataConnector(MDataConnector dataConnector) {
        this.dataConnector = dataConnector;
    }

    public int getCreateTime() {
        return createTime;
    }

    public void setCreateTime(int createTime) {
        this.createTime = createTime;
    }

    public String getGrantor() {
        return grantor;
    }

    public void setGrantor(String grantor) {
        this.grantor = grantor;
    }

    public String getGrantorType() {
        return grantorType;
    }

    public void setGrantorType(String grantorType) {
        this.grantorType = grantorType;
    }

    public boolean getGrantOption() {
        return grantOption;
    }

    public void setGrantOption(boolean grantOption) {
        this.grantOption = grantOption;
    }

    public String getPrincipalType() {
        return principalType;
    }

    public void setPrincipalType(String principalType) {
        this.principalType = principalType;
    }

    public String getAuthorizer() {
        return authorizer;
    }

    public void setAuthorizer(String authorizer) {
        this.authorizer = authorizer;
    }
}
