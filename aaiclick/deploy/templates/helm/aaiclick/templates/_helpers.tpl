{{/*
Connection URLs: env.sqlUrl / env.chUrl when set, else built from the
devDependencies credentials so each password is set in one place.
shortcut: urlquery encodes a space as "+", which a URL password reads as a
literal "+"; avoid spaces or set env.* yourself.
*/}}
{{- define "aaiclick.sqlUrl" -}}
{{- if .Values.env.sqlUrl -}}
{{- .Values.env.sqlUrl -}}
{{- else -}}
{{- with .Values.devDependencies.postgres -}}
postgresql+asyncpg://{{ .user | urlquery }}:{{ .password | urlquery }}@aaiclick-postgres:5432/{{ .db }}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "aaiclick.chUrl" -}}
{{- if .Values.env.chUrl -}}
{{- .Values.env.chUrl -}}
{{- else -}}
{{- with .Values.devDependencies.clickhouse -}}
clickhouse://{{ .user | urlquery }}:{{ .password | urlquery }}@aaiclick-clickhouse:8123/default
{{- end -}}
{{- end -}}
{{- end -}}
