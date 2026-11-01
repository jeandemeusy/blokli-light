{{- define "blokli-light.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "blokli-light.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "blokli-light.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
