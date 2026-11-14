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

{{/*
The GitHub App ID for private-repo builds, or empty while githubApp is off.
- appId and secretName go together: one without the other fails the render.
- `default dict`: `helm upgrade --reuse-values` from a release that predates
  the githubApp block has no such key.
- An unquoted number in values.yaml parses as a float, so print it whole.
*/}}
{{- define "aaiclick.githubAppId" -}}
{{- $app := .Values.githubApp | default dict -}}
{{- if ne (empty $app.appId) (empty $app.secretName) -}}
{{- fail "githubApp: set both appId and secretName, or neither" -}}
{{- end -}}
{{- if kindIs "float64" $app.appId -}}
{{- int64 $app.appId -}}
{{- else -}}
{{- $app.appId | default "" -}}
{{- end -}}
{{- end -}}
