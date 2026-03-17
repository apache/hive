#!/bin/sh -x

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e

apk add --no-cache jq
apk add --no-cache acl

#--------------------------------------------------------------------------------
# OBTAIN TOKEN
#--------------------------------------------------------------------------------

source /polaris/obtain-token.sh

echo
echo "Obtained access token: ${TOKEN}"

#--------------------------------------------------------------------------------
# CREATE CATALOG
#--------------------------------------------------------------------------------

echo
echo Creating a catalog named ice01 in realm $REALM...

STORAGE_TYPE="FILE"
STORAGE_LOCATION="file://${WAREHOUSE}"
STORAGE_CONFIG_INFO="{\"storageType\": \"$STORAGE_TYPE\", \"allowedLocations\": [\"$STORAGE_LOCATION\"]}"

PAYLOAD='{
   "catalog": {
     "name": "ice01",
     "type": "INTERNAL",
     "readOnly": false,
     "properties": {
       "default-base-location": "'$STORAGE_LOCATION'"
     },
     "storageConfigInfo": '$STORAGE_CONFIG_INFO'
   }
 }'

echo $PAYLOAD

curl -s -H "Authorization: Bearer ${TOKEN}" \
   -H 'Accept: application/json' \
   -H 'Content-Type: application/json' \
   -H "Polaris-Realm: $REALM" \
   http://polaris:8181/api/management/v1/catalogs \
   -d "$PAYLOAD" -v
   
#--------------------------------------------------------------------------------
# CREATE PRINCIPAL
#--------------------------------------------------------------------------------

echo
echo Creating a principal named 'ice01_principal' in realm $REALM...

PAYLOAD='{
    "principal": {
      "name": "ice01_principal"
    },
    "credentialRotationRequired": false
 }'

echo $PAYLOAD

curl -s -H "Authorization: Bearer ${TOKEN}" \
   -H 'Accept: application/json' \
   -H 'Content-Type: application/json' \
   -H "Polaris-Realm: $REALM" \
   http://polaris:8181/api/management/v1/principals \
   -d "$PAYLOAD" -v

#--------------------------------------------------------------------------------
# CREATE PRINCIPAL ROLE
#--------------------------------------------------------------------------------

PAYLOAD='{
    "principalRole": {
      "name": "ice01_principal_role"
    }
 }'
 
echo $PAYLOAD

curl -s -H "Authorization: Bearer ${TOKEN}" \
   -H 'Accept: application/json' \
   -H 'Content-Type: application/json' \
   -H "Polaris-Realm: $REALM" \
   http://polaris:8181/api/management/v1/principal-roles \
   -d "$PAYLOAD" -v
   
#--------------------------------------------------------------------------------
# CATALOG ROLE
#--------------------------------------------------------------------------------

PAYLOAD='{
  "catalogRole": {
    "name": "ice01_catalog_role"
  }
}'

echo $PAYLOAD

curl -s -H "Authorization: Bearer ${TOKEN}" \
   -H 'Accept: application/json' \
   -H 'Content-Type: application/json' \
   -H "Polaris-Realm: $REALM" \
   http://polaris:8181/api/management/v1/catalogs/ice01/catalog-roles \
   -d "$PAYLOAD" -v

#--------------------------------------------------------------------------------
# GRANT THE PRINCIPAL THE PRINCIPAL ROLE
#--------------------------------------------------------------------------------
   
PAYLOAD='{
  "principalRole": {
    "name": "ice01_principal_role"
  }
}'

echo $PAYLOAD

curl -s -X PUT -H "Authorization: Bearer ${TOKEN}" \
   -H 'Accept: application/json' \
   -H 'Content-Type: application/json' \
   -H "Polaris-Realm: $REALM" \
   http://polaris:8181/api/management/v1/principals/ice01_principal/principal-roles \
   -d "$PAYLOAD" -v
   
#--------------------------------------------------------------------------------
# GRANT THE CATALOG ROLE TO THE PRINCIPAL ROLE
#--------------------------------------------------------------------------------

PAYLOAD='{
  "type": "catalog", 
  "privilege": "CATALOG_MANAGE_CONTENT"
}'

echo $PAYLOAD

curl -s -X PUT -H "Authorization: Bearer ${TOKEN}" \
   -H 'Accept: application/json' \
   -H 'Content-Type: application/json' \
   -H "Polaris-Realm: $REALM" \
   http://polaris:8181/api/management/v1/catalogs/ice01/catalog-roles/ice01_catalog_role/grants \
   -d "$PAYLOAD" -v

#--------------------------------------------------------------------------------
# Create warehouse directory and set permissions
#--------------------------------------------------------------------------------

mkdir -p ${WAREHOUSE}
chmod 777 ${WAREHOUSE}

#--------------------------------------------------------------------------------
# Start ACL sync on the warehouse folder for hive and polaris users
#--------------------------------------------------------------------------------

while true
do
  # Existing files ACLs
  setfacl -R -m u:$HIVE_USER_ID:rwx,u:$POLARIS_USER_ID:rwx ${WAREHOUSE}

  # Default ACLs for new files
  setfacl -R -d -m u:$HIVE_USER_ID:rwx,u:$POLARIS_USER_ID:rwx ${WAREHOUSE}
done
