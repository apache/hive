{{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/* Chart name */}}
{{- define "hive-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fullname: release-name + chart-name */}}
{{- define "hive-operator.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Operator labels */}}
{{- define "hive-operator.labels" -}}
app.kubernetes.io/name: hive-kubernetes-operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end }}

{{/* Operator selector labels */}}
{{- define "hive-operator.selectorLabels" -}}
app.kubernetes.io/name: hive-kubernetes-operator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* ServiceAccount name */}}
{{- define "hive-operator.serviceAccountName" -}}
{{- printf "%s" (include "hive-operator.fullname" .) }}
{{- end }}

{{/* HiveCluster CR name */}}
{{- define "hive-operator.clusterName" -}}
{{- .Values.cluster.name | default .Release.Name }}
{{- end }}
