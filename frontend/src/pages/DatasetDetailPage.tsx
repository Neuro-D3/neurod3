import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import ReactMarkdown from 'react-markdown';
import { fetchDatasetDetail, fetchDatasetMetrics } from '../services/api';
import type { DatasetDetailResponse, DatasetMetrics } from '../services/api';
import { DatasetImpactCard } from '../components/DatasetImpactCard';
import { DatasetPaperList } from '../components/DatasetPaperList';
import { PaperMap } from '../components/PaperMap';
import {
  Button,
  ButtonLink,
  Card,
  CardTitle,
  Chip,
  CopyField,
  Eyebrow,
  ExternalIcon,
  FactTable,
  MastTag,
  Note,
  SubTitle,
  Tab,
  Tabs,
  cx,
} from '../components/ui';
import { buildPaperList } from '../utils/datasetPapers';
import type { DatasetPaperItem } from '../utils/datasetPapers';
import {
  archiveLibraries,
  caveats,
  citationText,
  contributorRoles,
  downloadCommand,
  qualifiedId,
  reuseTypeBreakdown,
  takeaways,
} from '../utils/datasetCard';
import { formatPublishedDate, plural } from '../utils/reuseMetrics';

// Short label for each junk reason (utils/dataset_status.py); title_keyword:<word> shows the word.
const JUNK_REASON_LABELS: Record<string, string> = {
  empty: 'empty dandiset',
  find_reuse_test_id: 'test ID',
  placeholder_title: 'placeholder title',
  description_phrase: 'test description',
  title_is_id: 'title is ID',
  no_title: 'no title',
};

const junkReasonLabel = (reason?: string | null): string =>
  reason?.startsWith('title_keyword:')
    ? `"${reason.slice('title_keyword:'.length)}" in title`
    : JUNK_REASON_LABELS[reason ?? ''] ?? 'junk';

function decodedDatasetIdParam(raw: string | undefined): string {
  if (!raw) return '';
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

function formatModality(label: string): string {
  return /[A-Z]{2,}/.test(label) ? label : label.toLowerCase();
}

/** Paper-map order: reuse first, then the dataset's own papers, then mentions; newest first within each. */
const MAP_ORDER = { REUSE: 0, PRIMARY: 1, MENTION: 2 } as const;
function paperMapOrder(items: DatasetPaperItem[]): DatasetPaperItem[] {
  return [...items].sort(
    (a, b) => MAP_ORDER[a.label] - MAP_ORDER[b.label] || (b.date ?? '').localeCompare(a.date ?? ''),
  );
}

type View = 'card' | 'papers';

/* ───────────────────────── Masthead ───────────────────────── */

function Masthead({ data, metricsTracked }: { data: DatasetDetailResponse; metricsTracked: boolean }) {
  const ds = data.dataset;
  const roles = contributorRoles(ds.contributors);
  const authors = ds.authors ?? [];
  const published = formatPublishedDate(ds.created_at, ds.created_at_precision);
  const updated = ds.updated_at ? new Date(ds.updated_at).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' }) : null;

  return (
    <header className="bg-navy text-white">
      <div className="mx-auto grid max-w-[1440px] items-center gap-x-8 gap-y-5 px-4 pb-6 pt-6 sm:px-6 lg:grid-cols-[minmax(0,1fr)_auto] lg:px-8">
        <div className="flex min-w-0 flex-col gap-2.5">
          <span className="font-mono text-xs uppercase tracking-[0.08em] text-navy-muted">NeuroD3 · Dataset card</span>
          <h1 className="m-0 text-[30px] font-bold leading-[1.1] tracking-[-0.005em] sm:text-[38px] lg:text-[46px]">{ds.title}</h1>
          {authors.length > 0 && (
            <p className="m-0 mt-0.5 text-[17px] leading-[1.4] text-white sm:text-[19px]">
              {authors.slice(0, 12).join(', ')}
              {authors.length > 12 && ` and ${plural(authors.length - 12, 'other', 'others')}`}
            </p>
          )}
          <div className="flex flex-wrap gap-x-6 gap-y-1.5 text-sm text-navy-sub">
            {roles.map((r) => (
              <span key={r.role}>
                {r.role}: {r.names.slice(0, 2).join(', ')}
                {r.names.length > 2 && ` +${r.names.length - 2}`}
              </span>
            ))}
            {ds.num_subjects != null && ds.num_subjects > 0 && (
              <span>
                {ds.num_subjects.toLocaleString()} {ds.source === 'OpenNeuro' ? 'participants' : 'subjects'}
              </span>
            )}
            {!metricsTracked && <span>Reuse tracking not yet available for {ds.source}</span>}
          </div>
        </div>
        <div className="flex flex-col gap-1 lg:items-end lg:text-right">
          <span className="font-mono text-[26px] font-bold tracking-[0.01em] sm:text-[30px]">{qualifiedId(ds.source, ds.dataset_id)}</span>
          <span className="text-sm text-navy-sub">
            {published && `Released ${published}`}
            {published && updated && ' · '}
            {updated && `Updated ${updated}`}
          </span>
          <span className="mt-1.5 flex flex-wrap gap-1.5 lg:justify-end">
            <MastTag>{ds.source}</MastTag>
            {ds.license && <MastTag title="License">{ds.license.replace(/^spdx:/i, '')}</MastTag>}
            {ds.dataset_status === 'excluded' && (
              <MastTag title={`Excluded as junk (${ds.dataset_status_reason ?? 'unknown reason'}). Hidden from the dataset list and counts.`}>
                Junk · {junkReasonLabel(ds.dataset_status_reason)}
              </MastTag>
            )}
          </span>
        </div>
      </div>
    </header>
  );
}

function MastheadSkeleton() {
  return (
    <header className="bg-navy text-white" aria-busy="true">
      <div className="mx-auto flex max-w-[1440px] flex-col gap-3 px-4 pb-7 pt-6 sm:px-6 lg:px-8">
        <span className="font-mono text-xs uppercase tracking-[0.08em] text-navy-muted">NeuroD3 · Dataset card</span>
        <div className="h-10 w-2/3 animate-skeleton rounded bg-white/20 motion-reduce:animate-none" />
        <div className="h-5 w-1/2 animate-skeleton rounded bg-white/15 motion-reduce:animate-none" />
      </div>
    </header>
  );
}

/* ───────────────────────── Band under the masthead ───────────────────────── */

function MetaBand({
  data,
  view,
  onView,
  paperCount,
}: {
  data: DatasetDetailResponse;
  view: View;
  onView: (v: View) => void;
  paperCount: number;
}) {
  const ds = data.dataset;
  const modalities = (ds.modality || '')
    .split(/[;,]/)
    .map((m) => m.trim())
    .filter(Boolean);
  const libraries = archiveLibraries(ds.source);
  const [cited, setCited] = useState(false);
  const citeTimer = useRef<number | null>(null);
  useEffect(() => () => {
    if (citeTimer.current !== null) window.clearTimeout(citeTimer.current);
  }, []);
  const cite = async () => {
    try {
      await navigator.clipboard.writeText(citationText(ds));
      setCited(true);
      if (citeTimer.current !== null) window.clearTimeout(citeTimer.current);
      citeTimer.current = window.setTimeout(() => setCited(false), 1500);
    } catch {
      setCited(false);
    }
  };

  return (
    <div className="border-b border-line bg-white">
      <div className="mx-auto flex max-w-[1440px] flex-col gap-2.5 px-4 pt-3.5 sm:px-6 lg:px-8">
        <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-3">
          <div className="flex flex-wrap items-center gap-x-5 gap-y-2">
            {modalities.length > 0 && (
              <div className="flex flex-wrap items-center gap-1.5">
                <Eyebrow className="mr-0.5">Modality</Eyebrow>
                {modalities.map((m) => (
                  <Chip key={m}>{formatModality(m)}</Chip>
                ))}
              </div>
            )}
            {ds.papers != null && (
              <div className="flex items-center gap-1.5">
                <Eyebrow className="mr-0.5">Primary papers</Eyebrow>
                <Chip>{ds.papers}</Chip>
              </div>
            )}
            {libraries.length > 0 && (
              <div className="flex items-center gap-1.5">
                <Eyebrow className="mr-0.5">Libraries</Eyebrow>
                {libraries.map((l) => (
                  <Chip key={l} mono>
                    {l}
                  </Chip>
                ))}
              </div>
            )}
          </div>
          <div className="flex shrink-0 gap-2">
            <Button variant="secondary" onClick={cite} aria-live="polite">
              {cited ? 'Citation copied' : 'Cite'}
            </Button>
            {ds.url && (
              <ButtonLink href={ds.url} target="_blank" rel="noopener noreferrer">
                View on {ds.source}
                <ExternalIcon />
              </ButtonLink>
            )}
          </div>
        </div>
        <Tabs label="Dataset views">
          <Tab active={view === 'card'} onClick={() => onView('card')}>
            Dataset card
          </Tab>
          <Tab active={view === 'papers'} onClick={() => onView('papers')} count={paperCount}>
            Papers
          </Tab>
        </Tabs>
      </div>
    </div>
  );
}

/* ───────────────────────── Cards ───────────────────────── */

function AboutCard({ data, metrics }: { data: DatasetDetailResponse; metrics: DatasetMetrics | null }) {
  const ds = data.dataset;
  const tracked = metrics?.tracked ? metrics : null;
  const modalities = (ds.modality || '')
    .split(/[;,]/)
    .map((m) => m.trim())
    .filter(Boolean);
  const description = ds.full_description || ds.description || '';
  const rows: Array<{ label: string; value: React.ReactNode }> = [];
  if (ds.num_subjects != null && ds.num_subjects > 0) {
    rows.push({ label: ds.source === 'OpenNeuro' ? 'Participants' : 'Subjects', value: ds.num_subjects.toLocaleString() });
  }
  if (modalities.length) rows.push({ label: 'Modality', value: modalities.map(formatModality).join(', ') });
  rows.push({ label: 'Primary papers', value: data.primary_papers.length.toLocaleString() });
  if (tracked) {
    rows.push({ label: 'Citing papers found', value: tracked.coverage.citing_papers.toLocaleString() });
    rows.push({ label: 'Reuse papers', value: tracked.reuse_count.toLocaleString() });
  }
  if (ds.license) rows.push({ label: 'License', value: ds.license.replace(/^spdx:/i, '') });
  const published = formatPublishedDate(ds.created_at, ds.created_at_precision);
  if (published) rows.push({ label: 'Released', value: published });

  return (
    <Card aria-labelledby="about-h">
      <CardTitle id="about-h">About This Dataset</CardTitle>
      {description ? (
        <div className="prose prose-sm max-w-none text-[15px] leading-[1.55] text-ink-2 prose-headings:text-ink prose-p:my-0 prose-p:text-ink-2 prose-a:text-link [&>*+*]:mt-3">
          <ReactMarkdown>{description}</ReactMarkdown>
        </div>
      ) : (
        <p className="m-0 text-sm italic text-muted">No description in the archive metadata.</p>
      )}
      <FactTable caption={qualifiedId(ds.source, ds.dataset_id)} rows={rows} />
    </Card>
  );
}

function UseCard({ data }: { data: DatasetDetailResponse }) {
  const ds = data.dataset;
  const command = downloadCommand(ds.source, ds.dataset_id);
  return (
    <Card aria-labelledby="use-h">
      <CardTitle id="use-h">Use This Dataset</CardTitle>
      {command ? (
        <CopyField value={command} label="Copy download command" />
      ) : ds.url ? (
        <CopyField value={ds.url} label="Copy dataset link" />
      ) : null}
      <div className="flex flex-col gap-1.5">
        <Eyebrow>Cite as</Eyebrow>
        <p className="m-0 rounded-[5px] bg-panel px-3.5 py-3 text-[13px] leading-[1.55] text-ink-2">{citationText(ds)}</p>
      </div>
    </Card>
  );
}

function CaveatsCard({ notes }: { notes: string[] }) {
  return (
    <Card aria-labelledby="cav-h">
      <CardTitle id="cav-h">Data and Caveats</CardTitle>
      <ul className="m-0 flex flex-col gap-2 pl-[18px] text-sm leading-normal text-ink-2">
        {notes.map((n) => (
          <li key={n}>{n}</li>
        ))}
      </ul>
    </Card>
  );
}

function TakeawaysCard({ items }: { items: string[] }) {
  return (
    <Card aria-labelledby="take-h">
      <CardTitle id="take-h">Takeaways</CardTitle>
      {items.length ? (
        <ul className="m-0 flex flex-col gap-2 pl-[18px] text-sm leading-normal text-ink-2">
          {items.map((t) => (
            <li key={t}>{t}</li>
          ))}
        </ul>
      ) : (
        <p className="m-0 text-sm italic text-muted">Nothing to say until citing papers have been classified.</p>
      )}
    </Card>
  );
}

function ReuseTypesCard({ papers, sameLabByDoi, number }: { papers: DatasetPaperItem[]; sameLabByDoi: Map<string, boolean>; number: number }) {
  const rows = reuseTypeBreakdown(papers, sameLabByDoi);
  const max = Math.max(1, ...rows.map((r) => Math.max(r.independent, r.sameLab)));
  const independent = rows.reduce((n, r) => n + r.independent, 0);
  const sameLab = rows.reduce((n, r) => n + r.sameLab, 0);
  return (
    <Card aria-labelledby="how-h">
      <CardTitle number={number} id="how-h">
        How It Is Reused
      </CardTitle>
      <span className="flex flex-wrap items-center gap-3.5 text-[12.5px] text-ink">
        <span className="flex items-center gap-1.5">
          <span className="h-2.5 w-3.5 bg-reuse" aria-hidden="true" />
          different lab (n = {independent})
        </span>
        <span className="flex items-center gap-1.5">
          <span className="h-2.5 w-3.5 bg-reuse-soft" aria-hidden="true" />
          same lab (n = {sameLab})
        </span>
      </span>
      <div
        role="img"
        aria-label={`Reuse papers by type, different lab then same lab: ${rows.map((r) => `${r.label} ${r.independent} and ${r.sameLab}`).join(', ')}.`}
        className="flex flex-col gap-[7px]"
      >
        {rows.map((r) => (
          <div key={r.key} className="grid items-center gap-2" style={{ gridTemplateColumns: '118px minmax(0, 1fr)' }}>
            <span className="text-right text-[12.5px] text-ink-2">{r.label}</span>
            <span className="flex flex-col gap-[2px]">
              <span className="flex items-center gap-1">
                <span className="h-[9px] bg-reuse" style={{ width: `${(100 * r.independent) / max}%` }} />
                <span className="text-[11px] tabular-nums text-ink-3">{r.independent}</span>
              </span>
              <span className="flex items-center gap-1">
                <span className="h-[9px] bg-reuse-soft" style={{ width: `${(100 * r.sameLab) / max}%` }} />
                <span className="text-[11px] tabular-nums text-ink-3">{r.sameLab}</span>
              </span>
            </span>
          </div>
        ))}
      </div>
      <Note>
        Reuse papers by what they did with the data, as labelled from their full text. One paper counts once, by its
        strongest label.
      </Note>
    </Card>
  );
}

function PeopleCard({ data }: { data: DatasetDetailResponse }) {
  const ds = data.dataset;
  const contributors = ds.contributors ?? [];
  const authors = ds.authors ?? [];
  if (!contributors.length && !authors.length) return null;
  return (
    <Card aria-labelledby="people-h">
      <CardTitle id="people-h">People</CardTitle>
      {contributors.length > 0 ? (
        <ul className="m-0 flex flex-col gap-1.5 p-0 text-sm">
          {contributors.map((c, i) => (
            <li key={`${c.name}-${i}`} className="flex items-baseline justify-between gap-3 border-t border-line-inner pt-1.5 first:border-t-0 first:pt-0">
              <span className="text-ink">{c.name}</span>
              {c.roles?.length > 0 && (
                <span className="shrink-0 text-right text-xs text-muted">
                  {c.roles.map((r) => r.replace('dcite:', '').replace(/([a-z])([A-Z])/g, '$1 $2')).join(', ')}
                </span>
              )}
            </li>
          ))}
        </ul>
      ) : (
        <>
          <SubTitle>Authors</SubTitle>
          <p className="m-0 text-sm leading-relaxed text-ink-2">{authors.join(', ')}</p>
        </>
      )}
    </Card>
  );
}

/* ───────────────────────── Page ───────────────────────── */

export default function DatasetDetailPage() {
  const { source: sourceParam, datasetId: datasetIdParam } = useParams<{ source: string; datasetId: string }>();
  const source = decodedDatasetIdParam(sourceParam);
  const datasetId = decodedDatasetIdParam(datasetIdParam);
  const navigate = useNavigate();
  const location = useLocation();
  const [data, setData] = useState<DatasetDetailResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [metrics, setMetrics] = useState<DatasetMetrics | null>(null);
  const [metricsError, setMetricsError] = useState<string | null>(null);

  // The view lives in the hash (#papers) so a link can open the paper list.
  const view: View = location.hash === '#papers' ? 'papers' : 'card';
  const setView = (v: View) => navigate({ hash: v === 'papers' ? '#papers' : '' }, { replace: true });

  useEffect(() => {
    if (!source || !datasetId) return;
    let cancelled = false;
    setLoading(true);
    setError(null);

    fetchDatasetDetail(source, datasetId)
      .then((res) => {
        if (!cancelled) setData(res);
      })
      .catch((err) => {
        if (!cancelled) setError(err.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [source, datasetId]);

  // Metrics load on their own so the page doesn't wait for them.
  useEffect(() => {
    if (!source || !datasetId) return;
    let cancelled = false;
    setMetrics(null);
    setMetricsError(null);

    fetchDatasetMetrics(source, datasetId)
      .then((res) => {
        if (!cancelled) setMetrics(res);
      })
      .catch((err) => {
        if (!cancelled) setMetricsError(err.message);
      });

    return () => { cancelled = true; };
  }, [source, datasetId]);

  const trackedMetrics = metrics?.tracked ? metrics : null;
  const metricsTracked = metrics ? metrics.tracked : true;
  const papers = useMemo(() => (data ? buildPaperList(data.primary_papers, data.citations) : []), [data]);
  const mapPapers = useMemo(() => paperMapOrder(papers), [papers]);
  const sameLabByDoi = useMemo(
    () => new Map((trackedMetrics?.reuse_papers ?? []).map((p) => [p.doi, p.same_lab])),
    [trackedMetrics],
  );

  if (loading) {
    return (
      <div className="min-h-screen bg-canvas">
        <MastheadSkeleton />
        <div className="h-[92px] border-b border-line bg-white" />
        <div className="mx-auto grid max-w-[1440px] gap-6 px-4 py-6 sm:px-6 lg:grid-cols-3 lg:px-8" aria-busy="true">
          {[0, 1, 2].map((i) => (
            <div key={i} className="h-72 animate-skeleton rounded-md border border-line bg-white motion-reduce:animate-none" />
          ))}
        </div>
      </div>
    );
  }

  if (error || !data) {
    const notFound = error?.includes('not found');
    return (
      <div className="min-h-screen bg-canvas px-4 py-16">
        <Card className="mx-auto max-w-md text-center">
          <CardTitle>{notFound ? 'Dataset not found' : 'Something went wrong'}</CardTitle>
          <p className="m-0 text-sm text-ink-2">
            {notFound ? `No ${source.toUpperCase()} dataset with the id ${datasetId} is in the hub.` : error}
          </p>
          <div className="flex justify-center gap-2">
            <Button variant="secondary" onClick={() => navigate(-1)}>
              Back
            </Button>
            <ButtonLink to="/">All datasets</ButtonLink>
          </div>
        </Card>
      </div>
    );
  }

  const ds = data.dataset;
  const notes = caveats(ds, trackedMetrics, metricsTracked, data.citations.length);
  const points = takeaways(ds, trackedMetrics, papers);
  const hasReuse = papers.some((p) => p.label === 'REUSE');
  const impactLoading = !metrics && !metricsError;

  return (
    <div className="min-h-screen bg-canvas">
      <Masthead data={data} metricsTracked={metricsTracked} />
      <MetaBand data={data} view={view} onView={setView} paperCount={papers.length} />

      {view === 'papers' ? (
        <div className="mx-auto grid max-w-[1440px] gap-6 px-4 py-6 sm:px-6 lg:grid-cols-[minmax(0,2fr)_minmax(0,1fr)] lg:px-8">
          <DatasetPaperList items={papers} metrics={trackedMetrics} />
          <div className="flex flex-col gap-6">
            <DatasetImpactCard
              metrics={trackedMetrics}
              loading={impactLoading}
              error={metricsError}
              untrackedNote={`Reuse metrics are not tracked for ${ds.source} yet.`}
            />
            <CaveatsCard notes={notes} />
          </div>
        </div>
      ) : (
        <div
          className={cx(
            'mx-auto grid max-w-[1440px] items-start gap-6 px-4 py-6 sm:px-6 lg:px-8',
            'md:grid-cols-2 xl:grid-cols-[minmax(0,5fr)_minmax(0,7fr)_minmax(0,5fr)]',
          )}
        >
          <div className="flex flex-col gap-6">
            <AboutCard data={data} metrics={metrics} />
            <UseCard data={data} />
            <CaveatsCard notes={notes} />
          </div>
          <div className="flex flex-col gap-6">
            <PaperMap papers={mapPapers} sameLabByDoi={sameLabByDoi} totalLinked={papers.length} onShowAll={() => setView('papers')} />
            <DatasetImpactCard
              metrics={trackedMetrics}
              loading={impactLoading}
              error={metricsError}
              untrackedNote={`Reuse metrics are not tracked for ${ds.source} yet: no citing papers have been mapped for this archive.`}
            />
          </div>
          <div className="flex flex-col gap-6 md:col-span-2 md:grid md:grid-cols-2 md:gap-6 xl:col-span-1 xl:flex">
            {hasReuse && <ReuseTypesCard papers={papers} sameLabByDoi={sameLabByDoi} number={3} />}
            <TakeawaysCard items={points} />
            <PeopleCard data={data} />
          </div>
        </div>
      )}
    </div>
  );
}
