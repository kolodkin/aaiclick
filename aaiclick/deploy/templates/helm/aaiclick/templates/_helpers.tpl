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
Setting only one of githubApp.appId / githubApp.secretName fails the render.
`default dict` covers `helm upgrade --reuse-values` from a release that
predates the githubApp block; an unquoted number in values.yaml parses as a
float, so it is printed whole.
*/}}
{{- define "aaiclick.githubAppId" -}}
{{- $app := .Values.githubApp | default dict -}}
{{- if and $app.appId (not $app.secretName) -}}
{{- fail "githubApp.appId is set but githubApp.secretName is empty: set both, or neither" -}}
{{- else if and $app.secretName (not $app.appId) -}}
{{- fail "githubApp.secretName is set but githubApp.appId is empty: set both, or neither" -}}
{{- else if kindIs "float64" $app.appId -}}
{{- int64 $app.appId -}}
{{- else -}}
{{- $app.appId | default "" -}}
{{- end -}}
{{- end -}}
