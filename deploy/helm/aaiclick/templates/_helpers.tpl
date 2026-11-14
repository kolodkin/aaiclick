{{/*
Connection URLs: env.sqlUrl / env.chUrl when set, else built from the
devDependencies credentials. Called from credentials.yaml with the resolved
password: dict "root" $ "password" $pw.
shortcut: urlquery encodes a space as "+", which a URL password reads as a
literal "+"; avoid spaces or set env.* yourself.
*/}}
{{- define "aaiclick.sqlUrl" -}}
{{- if .root.Values.env.sqlUrl -}}
{{- .root.Values.env.sqlUrl -}}
{{- else -}}
{{- with .root.Values.devDependencies.postgres -}}
postgresql+asyncpg://{{ .user | urlquery }}:{{ $.password | urlquery }}@aaiclick-postgres:5432/{{ .db }}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "aaiclick.chUrl" -}}
{{- if .root.Values.env.chUrl -}}
{{- .root.Values.env.chUrl -}}
{{- else -}}
{{- with .root.Values.devDependencies.clickhouse -}}
clickhouse://{{ .user | urlquery }}:{{ $.password | urlquery }}@aaiclick-clickhouse:8123/default
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Name of the Secret every credential is read from: auth.existingSecret, or
the one credentials.yaml generates.
*/}}
{{- define "aaiclick.credentialsSecret" -}}
{{- .Values.auth.existingSecret | default (printf "%s-credentials" .Release.Name) -}}
{{- end -}}

{{/*
An env entry read from the credentials Secret: dict "root" $ "name" ENV "key" KEY.
*/}}
{{- define "aaiclick.secretEnv" -}}
- name: {{ .name }}
  valueFrom:
    secretKeyRef:
      name: {{ include "aaiclick.credentialsSecret" .root }}
      key: {{ .key }}
{{- end -}}

{{/*
The database URL env pair every aaiclick container reads.
*/}}
{{- define "aaiclick.dbEnv" -}}
{{ include "aaiclick.secretEnv" (dict "root" . "name" "AAICLICK_SQL_URL" "key" "sql-url") }}
{{ include "aaiclick.secretEnv" (dict "root" . "name" "AAICLICK_CH_URL" "key" "ch-url") }}
{{- end -}}

{{/*
Image reference; an empty tag means the chart's appVersion:
dict "root" $ "image" .Values.images.server.
*/}}
{{- define "aaiclick.image" -}}
{{ .image.repository }}:{{ .image.tag | default .root.Chart.AppVersion }}
{{- end -}}

{{/*
Rolls a Deployment when a credential input changes. Hashes the values, not the
rendered Secret: a fresh install renders random values per include, which
would roll every pod on the first no-op upgrade.
*/}}
{{- define "aaiclick.credentialsChecksum" -}}
{{- $dev := .Values.devDependencies -}}
{{- $inputs := list .Values.auth .Values.env.sqlUrl .Values.env.chUrl $dev.enabled $dev.postgres.password $dev.clickhouse.password -}}
checksum/credentials: {{ $inputs | toJson | sha256sum }}
{{- end -}}

{{/*
The GitHub App ID for private-repo builds, or empty while githubApp is off.
- appId and secretName go together: one without the other fails the render.
- `default dict`: `helm upgrade --reuse-values` from a release that predates
  the githubApp block has no such key.
*/}}
{{- define "aaiclick.githubAppId" -}}
{{- $app := .Values.githubApp | default dict -}}
{{- if ne (empty $app.appId) (empty $app.secretName) -}}
{{- fail "githubApp: set both appId and secretName, or neither" -}}
{{- end -}}
{{- $app.appId | default "" -}}
{{- end -}}
