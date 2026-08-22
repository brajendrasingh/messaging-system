{{- define "kafka-kraft.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kafka-kraft.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "kafka-kraft.kafkaService" -}}
{{- printf "%s-headless" (include "kafka-kraft.fullname" .) }}
{{- end }}

{{- define "kafka-kraft.kafkaUi" -}}
{{- printf "%s-ui" (include "kafka-kraft.fullname" .) }}
{{- end }}