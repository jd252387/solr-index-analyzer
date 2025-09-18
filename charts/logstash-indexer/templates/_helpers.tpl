{{- define "logstash-indexer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "logstash-indexer.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "logstash-indexer.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version -}}
{{- end -}}

{{- define "logstash-indexer.labels" -}}
helm.sh/chart: {{ include "logstash-indexer.chart" . }}
{{ include "logstash-indexer.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end -}}

{{- define "logstash-indexer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "logstash-indexer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "logstash-indexer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{ default (include "logstash-indexer.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{- define "logstash-indexer.toFieldRef" -}}
{{- $field := . | default "" -}}
{{- if eq $field "" -}}
{{- "" -}}
{{- else if hasPrefix $field "[" -}}
{{- $field -}}
{{- else -}}
{{- $normalized := regexReplaceAll "\\." $field "][" -}}
[{{ $normalized }}]
{{- end -}}
{{- end -}}

{{- define "logstash-indexer.appendField" -}}
{{- $base := include "logstash-indexer.toFieldRef" .base -}}
{{- $fieldRef := include "logstash-indexer.toFieldRef" .field -}}
{{- if eq $base "" -}}
{{- $fieldRef -}}
{{- else if eq $fieldRef "" -}}
{{- $base -}}
{{- else -}}
{{- $trimmed := trimSuffix "]" (trimPrefix "[" $fieldRef) -}}
{{ printf "%s[%s]" $base $trimmed }}
{{- end -}}
{{- end -}}

{{- define "logstash-indexer.jsonPointerField" -}}
{{- $base := .base | default "" -}}
{{- $pointer := .pointer | default "" -}}
{{- $field := $base -}}
{{- $trimmed := trimPrefix "/" $pointer -}}
{{- if ne $trimmed "" -}}
{{- $segments := splitList "/" $trimmed -}}
{{- range $segment := $segments -}}
{{- $decoded := replace (replace $segment "~1" "/") "~0" "~" -}}
{{- if eq $field "" -}}
{{- $field = include "logstash-indexer.toFieldRef" $decoded -}}
{{- else -}}
{{- $field = include "logstash-indexer.appendField" (dict "base" $field "field" $decoded) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if eq $field "" -}}
{{- $field = $base -}}
{{- end -}}
{{- $field -}}
{{- end -}}

{{- define "logstash-indexer.renderValue" -}}
{{- $v := . -}}
{{- if kindIs "string" $v -}}
"{{ $v }}"
{{- else if kindIs "bool" $v -}}
{{- if $v }}true{{ else }}false{{ end -}}
{{- else if or (kindIs "int64" $v) (kindIs "float64" $v) (kindIs "float32" $v) (kindIs "int" $v) -}}
{{ $v }}
{{- else if kindIs "slice" $v -}}
[{{- $first := true -}}{{- range $item := $v -}}{{- if $first -}}{{- $first = false -}}{{ else }}, {{ end -}}{{ include "logstash-indexer.renderValue" $item }}{{- end -}}]
{{- else if kindIs "map" $v -}}
{{- include "logstash-indexer.renderHash" $v -}}
{{- else -}}
"{{ $v }}"
{{- end -}}
{{- end -}}

{{- define "logstash-indexer.renderHash" -}}
{{- $map := . -}}
{{- if $map }}
{ {{- $first := true -}}{{- range $k, $val := $map -}}{{- if $first -}}{{- $first = false -}}{{ else }}, {{ end -}}"{{ $k }}" => {{ include "logstash-indexer.renderValue" $val }}{{- end -}} }
{{- else -}}
{ }
{{- end -}}
{{- end -}}

{{- define "logstash-indexer.renderList" -}}
[{{- $first := true -}}{{- range $item := . -}}{{- if $first -}}{{- $first = false -}}{{ else }}, {{ end -}}{{ include "logstash-indexer.renderValue" $item }}{{- end -}}]
{{- end -}}

{{- define "logstash-indexer.fieldPresenceCondition" -}}
{{- $fields := . | default (list) -}}
{{- $first := true -}}
{{- range $field := $fields -}}
{{- $ref := include "logstash-indexer.toFieldRef" $field -}}
{{- if $ref }}
{{- if $first -}}{{- $first = false -}}{{ else }} and {{ end -}}{{ $ref }}
{{- end -}}
{{- end -}}
{{- if $first }}false{{- end -}}
{{- end -}}

{{- define "logstash-indexer.sourceTypeCondition" -}}
{{- $field := include "logstash-indexer.toFieldRef" .field -}}
{{- $values := .values | default (list) -}}
{{- if and $field (gt (len $values) 0) -}}
{{- $first := true -}}
{{- range $val := $values -}}
{{- if ne (printf "%v" $val) "" -}}
{{- if $first -}}{{- $first = false -}}{{ else }} or {{ end -}}{{ printf "%s == \"%s\"" $field $val }}
{{- end -}}
{{- end -}}
{{- if $first }}false{{- end -}}
{{- else -}}
false
{{- end -}}
{{- end -}}

{{- define "logstash-indexer.renderSecretEnv" -}}
{{- $cfg := . -}}
{{- if and $cfg.envVar (or $cfg.value (and $cfg.existingSecret $cfg.key)) }}
- name: {{ $cfg.envVar }}
  {{- if $cfg.value }}
  value: {{ $cfg.value | quote }}
  {{- else }}
  valueFrom:
    secretKeyRef:
      name: {{ $cfg.existingSecret }}
      key: {{ $cfg.key }}
  {{- end }}
{{- end -}}
{{- end -}}
