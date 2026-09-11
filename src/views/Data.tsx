import { columnNames, columnTypes, renderCell, ResultsTable } from "@qv/core";
import { useObjectRows, useObjects } from "../api/hooks";
import type { ObjectView } from "../api/types";
import { Chips } from "../components/Chips";
import { MetaGrid } from "../components/MetaGrid";
import { ObjectsTable } from "../components/ObjectsTable";
import { ScopeTree } from "../components/ScopeTree";
import { relativeTime } from "../lib/format";
import { dataPrompt, formatBytes, queryPrompt, scopeKey, scopeLabel } from "../lib/viewer";

export function Data({
  job,
  object,
  onPrompt,
}: {
  job: string | null;
  object: string | null;
  onPrompt: (v: string) => void;
}) {
  const scope = scopeKey(job);
  const objects = useObjects(scope);
  const selected = object ? objects.data?.items.find((o) => o.name === object) : undefined;

  return (
    <>
      <h2>Data</h2>
      <p className="sub">Objects of the selected scope · click a row to preview</p>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@query", cmd: queryPrompt(scope, object ?? undefined) },
          { label: "@dashboard", cmd: "@dashboard" },
        ]}
        onPrompt={onPrompt}
      />
      <div className="viewer-layout">
        <ScopeTree scope={scope} onScope={(s) => onPrompt(dataPrompt(s))} />
        <div className="viewer-main">
          {objects.isLoading && <p className="sub">loading…</p>}
          {objects.isError && <p className="err">failed to load objects</p>}
          {object ? (
            <ObjectPreview scope={scope} name={object} view={selected} onPrompt={onPrompt} />
          ) : (
            objects.data && <ObjectsTable objects={objects.data.items} scope={scope} onPrompt={onPrompt} />
          )}
        </div>
      </div>
    </>
  );
}

function ObjectPreview({
  scope,
  name,
  view,
  onPrompt,
}: {
  scope: string;
  name: string;
  view: ObjectView | undefined;
  onPrompt: (v: string) => void;
}) {
  const query = useObjectRows(scope, name);
  const rows = query.data ?? null;
  const columns = rows ? columnNames(rows) : [];
  const colTypes = rows ? columnTypes(rows) : {};

  return (
    <>
      <div className="detail-head">
        <div className="title-row">
          <h2>
            <span className="mono">{name}</span>
          </h2>
          <div className="spacer" />
          <button
            type="button"
            className="btn btn-sm"
            data-testid="preview-query"
            onClick={() => onPrompt(queryPrompt(scope, name))}
          >
            Query
          </button>
        </div>
        <MetaGrid
          items={[
            { k: "Scope", v: scopeLabel(scope) },
            { k: "Rows", v: view?.row_count ?? "—", mono: true },
            { k: "Size", v: formatBytes(view?.size_bytes) },
            { k: "Created", v: relativeTime(view?.created_at) },
          ]}
        />
      </div>
      <Chips chips={[{ label: `← ${scopeLabel(scope)}`, cmd: dataPrompt(scope) }]} onPrompt={onPrompt} />
      {query.isPending && <p className="sub">loading rows…</p>}
      {query.isError && <p className="err">{query.error.message}</p>}
      {rows && (
        <ResultsTable
          columns={columns}
          rows={rows.data}
          shownIdx={columns.map((_, i) => i)}
          testid="object-rows"
          renderCell={(col, value, row) => renderCell(col, value, {}, row, columns, colTypes)}
        />
      )}
    </>
  );
}
