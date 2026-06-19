{{/*
Expand the name of the chart.
*/}}
{{- define "ublk-azblob-csi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "ublk-azblob-csi.fullname" -}}
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
{{- define "ublk-azblob-csi.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ublk-azblob-csi.labels" -}}
helm.sh/chart: {{ include "ublk-azblob-csi.chart" . }}
{{ include "ublk-azblob-csi.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ublk-azblob-csi.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ublk-azblob-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Controller labels
*/}}
{{- define "ublk-azblob-csi.controller.labels" -}}
{{ include "ublk-azblob-csi.labels" . }}
app.kubernetes.io/component: controller
{{- end }}

{{/*
Node labels
*/}}
{{- define "ublk-azblob-csi.node.labels" -}}
{{ include "ublk-azblob-csi.labels" . }}
app.kubernetes.io/component: node
{{- end }}

{{/*
Controller ServiceAccount name
*/}}
{{- define "ublk-azblob-csi.controller.serviceAccountName" -}}
{{- if .Values.serviceAccount.controller.create }}
{{- default "csi-ublk-azblob-controller" .Values.serviceAccount.controller.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.controller.name }}
{{- end }}
{{- end }}

{{/*
Node ServiceAccount name
*/}}
{{- define "ublk-azblob-csi.node.serviceAccountName" -}}
{{- if .Values.serviceAccount.node.create }}
{{- default "csi-ublk-azblob-node" .Values.serviceAccount.node.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.node.name }}
{{- end }}
{{- end }}

{{/*
Secret name based on deployment mode
*/}}
{{- define "ublk-azblob-csi.secretName" -}}
{{- if eq .Values.secretSearchMode "global" }}
{{- .Values.globalSecret.name }}
{{- else }}
{{- .Values.perNamespaceSecret.name }}
{{- end }}
{{- end }}

{{/*
Secret namespace based on deployment mode
*/}}
{{- define "ublk-azblob-csi.secretNamespace" -}}
{{- if eq .Values.secretSearchMode "global" -}}
{{- .Values.namespace -}}
{{- else -}}
${pvc.namespace}
{{- end -}}
{{- end -}}
