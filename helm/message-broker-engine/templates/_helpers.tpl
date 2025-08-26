{{/*
Expand the name of the chart.
*/}}
{{- define "message-broker-engine.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "message-broker-engine.fullname" -}}
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
{{- define "message-broker-engine.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "message-broker-engine.labels" -}}
helm.sh/chart: {{ include "message-broker-engine.chart" . }}
{{ include "message-broker-engine.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: trading-platform
app.kubernetes.io/component: message-broker
{{- end }}

{{/*
Selector labels
*/}}
{{- define "message-broker-engine.selectorLabels" -}}
app.kubernetes.io/name: {{ include "message-broker-engine.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "message-broker-engine.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "message-broker-engine.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Generate Redis connection URL
*/}}
{{- define "message-broker-engine.redisUrl" -}}
{{- if .Values.redis.external.enabled }}
{{- .Values.redis.external.host }}:{{ .Values.redis.external.port }}
{{- else }}
{{- include "message-broker-engine.fullname" . }}-redis:6379
{{- end }}
{{- end }}

{{/*
Generate Redis auth secret name
*/}}
{{- define "message-broker-engine.redisSecretName" -}}
{{- if .Values.redis.external.existingSecret }}
{{- .Values.redis.external.existingSecret }}
{{- else }}
{{- include "message-broker-engine.fullname" . }}-redis-auth
{{- end }}
{{- end }}

{{/*
Generate TLS secret name for Redis
*/}}
{{- define "message-broker-engine.redisTlsSecretName" -}}
{{- if .Values.redis.tls.existingSecret }}
{{- .Values.redis.tls.existingSecret }}
{{- else }}
{{- include "message-broker-engine.fullname" . }}-redis-tls
{{- end }}
{{- end }}

{{/*
Common pod security context
*/}}
{{- define "message-broker-engine.podSecurityContext" -}}
{{- if .Values.podSecurityContext }}
{{- toYaml .Values.podSecurityContext }}
{{- else }}
runAsNonRoot: true
runAsUser: 65534
runAsGroup: 65534
fsGroup: 65534
seccompProfile:
  type: RuntimeDefault
{{- end }}
{{- end }}

{{/*
Common container security context
*/}}
{{- define "message-broker-engine.securityContext" -}}
{{- if .Values.securityContext }}
{{- toYaml .Values.securityContext }}
{{- else }}
allowPrivilegeEscalation: false
runAsNonRoot: true
runAsUser: 65534
runAsGroup: 65534
readOnlyRootFilesystem: true
capabilities:
  drop:
    - ALL
{{- end }}
{{- end }}

{{/*
Generate environment variables for configuration
*/}}
{{- define "message-broker-engine.env" -}}
- name: RUST_LOG
  value: {{ .Values.config.logging.level | quote }}
- name: SERVER_HOST
  value: {{ .Values.config.server.host | quote }}
- name: SERVER_PORT
  value: {{ .Values.config.server.port | quote }}
- name: REDIS_URL
  value: "redis://{{ include "message-broker-engine.redisUrl" . }}"
{{- if .Values.redis.password }}
- name: REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "message-broker-engine.redisSecretName" . }}
      key: redis-password
{{- end }}
{{- if .Values.config.performance.worker_threads }}
- name: TOKIO_WORKER_THREADS
  value: {{ .Values.config.performance.worker_threads | quote }}
{{- end }}
- name: METRICS_ENABLED
  value: {{ .Values.metrics.enabled | quote }}
{{- if .Values.metrics.enabled }}
- name: METRICS_PORT
  value: {{ .Values.metrics.port | quote }}
{{- end }}
{{- if .Values.config.tls.enabled }}
- name: TLS_CERT_PATH
  value: "/etc/tls/tls.crt"
- name: TLS_KEY_PATH
  value: "/etc/tls/tls.key"
{{- end }}
{{- range $key, $value := .Values.config.env }}
- name: {{ $key }}
  value: {{ $value | quote }}
{{- end }}
{{- end }}

{{/*
Generate volume mounts
*/}}
{{- define "message-broker-engine.volumeMounts" -}}
- name: config
  mountPath: /etc/config
  readOnly: true
- name: tmp
  mountPath: /tmp
{{- if and .Values.config.logging.enable_file_logging .Values.persistence.logs.enabled }}
- name: logs
  mountPath: /var/log/message-broker
{{- end }}
{{- if .Values.persistence.enabled }}
- name: data
  mountPath: /data
{{- end }}
{{- if .Values.config.tls.enabled }}
- name: tls
  mountPath: /etc/tls
  readOnly: true
{{- end }}
{{- end }}

{{/*
Generate volumes
*/}}
{{- define "message-broker-engine.volumes" -}}
- name: config
  configMap:
    name: {{ include "message-broker-engine.fullname" . }}
- name: tmp
  emptyDir:
    sizeLimit: 1Gi
{{- if and .Values.config.logging.enable_file_logging .Values.persistence.logs.enabled }}
- name: logs
  persistentVolumeClaim:
    claimName: {{ include "message-broker-engine.fullname" . }}-logs
{{- end }}
{{- if .Values.persistence.enabled }}
- name: data
  persistentVolumeClaim:
    claimName: {{ include "message-broker-engine.fullname" . }}-data
{{- end }}
{{- if .Values.config.tls.enabled }}
- name: tls
  secret:
    secretName: {{ .Values.config.tls.secretName | default (printf "%s-tls" (include "message-broker-engine.fullname" .)) }}
    defaultMode: 0400
{{- end }}
{{- end }}

{{/*
Generate resource requirements
*/}}
{{- define "message-broker-engine.resources" -}}
{{- if .Values.resources }}
{{- toYaml .Values.resources }}
{{- else }}
requests:
  cpu: 100m
  memory: 128Mi
limits:
  cpu: 500m
  memory: 512Mi
{{- end }}
{{- end }}
