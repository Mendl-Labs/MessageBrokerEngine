{{/*
Expand the name of the chart.
*/}}
{{- define "ultra-message-broker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "ultra-message-broker.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "ultra-message-broker.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ultra-message-broker.labels" -}}
helm.sh/chart: {{ include "ultra-message-broker.chart" . }}
{{ include "ultra-message-broker.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: message-broker
app.kubernetes.io/part-of: trading-platform
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ultra-message-broker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ultra-message-broker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "ultra-message-broker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "ultra-message-broker.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the config map
*/}}
{{- define "ultra-message-broker.configMapName" -}}
{{- printf "%s-config" (include "ultra-message-broker.fullname" .) }}
{{- end }}

{{/*
Create the name of the persistent volume claim
*/}}
{{- define "ultra-message-broker.pvcName" -}}
{{- printf "%s-data" (include "ultra-message-broker.fullname" .) }}
{{- end }}

{{/*
Create the selector for network policy
*/}}
{{- define "ultra-message-broker.networkPolicySelector" -}}
matchLabels:
  {{ include "ultra-message-broker.selectorLabels" . | nindent 2 }}
{{- end }}

{{/*
Create performance annotations
*/}}
{{- define "ultra-message-broker.performanceAnnotations" -}}
{{- if .Values.performance.cpuAffinity }}
scheduler.alpha.kubernetes.io/critical-pod: ""
scheduler.alpha.kubernetes.io/preferred-max-skew: "1"
{{- end }}
{{- if .Values.performance.numaAware }}
numa.kubernetes.io/topology-policy: "single-numa-node"
{{- end }}
{{- end }}

{{/*
Aliases for message-broker-engine templates
*/}}
{{- define "message-broker-engine.name" -}}
{{- include "ultra-message-broker.name" . }}
{{- end }}

{{- define "message-broker-engine.fullname" -}}
{{- include "ultra-message-broker.fullname" . }}
{{- end }}

{{- define "message-broker-engine.chart" -}}
{{- include "ultra-message-broker.chart" . }}
{{- end }}

{{- define "message-broker-engine.labels" -}}
{{- include "ultra-message-broker.labels" . }}
{{- end }}

{{- define "message-broker-engine.selectorLabels" -}}
{{- include "ultra-message-broker.selectorLabels" . }}
{{- end }}

{{- define "message-broker-engine.serviceAccountName" -}}
{{- include "ultra-message-broker.serviceAccountName" . }}
{{- end }}
