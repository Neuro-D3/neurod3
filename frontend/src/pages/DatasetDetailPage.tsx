import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import ReactMarkdown from 'react-markdown';
import { fetchDatasetDetail, fetchDatasetMetrics } from '../services/api';
import type { DatasetDetailResponse, DatasetMetrics, TrackedDatasetMetrics } from '../services/api';
import { DatasetImpactCard } from '../components/DatasetImpactCard';
import { DatasetPaperList } from '../components/DatasetPaperList';
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
  cx,
} from '../components/ui';
import { buildPaperList, countByLabel } from '../utils/datasetPapers';
import type { DatasetPaperItem } from '../utils/datasetPapers';
import {
  CITATION_LINKS_LOADED,
  archiveLibraries,
  caveats,
  citationText,
  contributorRoles,
  downloadCommand,
  peopleView,
  qualifiedId,
  reuseTypeBreakdown,
  takeaways,
} from '../utils/datasetCard';
import { licenseLinks } from '../utils/license';
import { formatDay, formatPublishedDate, plural } from '../utils/reuseMetrics';

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

/* ───────────────────────── Masthead ───────────────────────── */

/** Back to wherever the visitor came from (the filtered list), or the list itself when they arrived here directly. */
function useGoBack() {
  const navigate = useNavigate();
  return () => {
    const idx = (window.history.state as { idx?: number } | null)?.idx ?? 0;
    if (idx > 0) navigate(-1);
    else navigate('/');
  };
}

function BackButton() {
  const goBack = useGoBack();
  return (
    <button
      type="button"
      onClick={goBack}
      className="inline-flex items-center gap-1.5 font-mono text-xs uppercase tracking-[0.08em] text-navy-muted hover:text-white"
    >
      <span aria-hidden="true">←</span> Datasets
    </button>
  );
}

function Masthead({
  data,
  metrics,
  metricsTracked,
  routeId,
}: {
  data: DatasetDetailResponse;
  metrics: TrackedDatasetMetrics | null;
  metricsTracked: boolean;
  routeId: string;
}) {
  const ds = data.dataset;
  const [allPeople, setAllPeople] = useState(false);
  const people = peopleView(ds.authors ?? [], contributorRoles(ds.contributors), allPeople);
  const published = formatPublishedDate(ds.created_at, ds.created_at_precision);
  const updated = formatDay(ds.updated_at);
  const licenses = licenseLinks(ds.license);
  const routeIdDiffersFromApi = Boolean(routeId && routeId !== ds.dataset_id);
  const showReuseSummary = Boolean(metrics && metrics.reuse_count + metrics.mention_count > 0);
  const toggleClass = 'text-navy-sub underline decoration-navy-tag underline-offset-2 hover:text-white';

  return (
    <header className="bg-navy text-white">
      <div className="mx-auto grid max-w-[1440px] items-center gap-x-8 gap-y-5 px-4 pb-6 pt-5 sm:px-6 lg:grid-cols-[minmax(0,1fr)_auto] lg:px-8">
        <div className="flex min-w-0 flex-col gap-2.5">
          <span className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <BackButton />
            <span aria-hidden="true" className="text-navy-tag">/</span>
            <span className="font-mono text-xs uppercase tracking-[0.08em] text-navy-muted">Dataset card</span>
          </span>
          <h1 className="m-0 break-words text-[30px] font-bold leading-[1.1] tracking-[-0.005em] sm:text-[38px] lg:text-[46px]">{ds.title}</h1>
          {people.authors.length > 0 && (
            <p className="m-0 mt-0.5 text-[17px] leading-[1.4] text-white sm:text-[19px]">
              {people.authors.join(', ')}
              {people.hiddenAuthors > 0 && ` and ${plural(people.hiddenAuthors, 'other', 'others')}`}
            </p>
          )}
          <div id="dataset-people" className="flex flex-wrap gap-x-6 gap-y-1.5 text-sm text-navy-sub">
            {people.roles.map((r) => (
              <span key={r.role}>
                {r.role}: {r.names.join(', ')}
                {r.more > 0 && ` +${r.more}`}
              </span>
            ))}
            {people.hiddenRoles > 0 && <span>{plural(people.hiddenRoles, 'more role', 'more roles')}</span>}
            {(people.truncated || allPeople) && (
              <button
                type="button"
                onClick={() => setAllPeople((v) => !v)}
                aria-expanded={allPeople}
                aria-controls="dataset-people"
                className={toggleClass}
              >
                {allPeople ? 'Show fewer people' : `Show all ${people.people.toLocaleString('en-US')} people`}
              </button>
            )}
            {ds.num_subjects != null && ds.num_subjects > 0 && (
              <span>
                {ds.num_subjects.toLocaleString('en-US')} {ds.source === 'OpenNeuro' ? 'participants' : 'subjects'}
              </span>
            )}
            {!metricsTracked && <span>Reuse tracking not yet available for {ds.source}</span>}
          </div>
        </div>
        <div className="flex flex-col gap-1 lg:items-end lg:text-right">
          <span className="font-mono text-[26px] font-bold tracking-[0.01em] sm:text-[30px]">{qualifiedId(ds.source, ds.dataset_id)}</span>
          {routeIdDiffersFromApi && (
            <span className="font-mono text-xs text-navy-muted" title="The id in this page's address">
              URL id: {routeId}
            </span>
          )}
          <span className="text-sm text-navy-sub">
            {published && `Released ${published}`}
            {published && updated && ' · '}
            {updated && `Updated ${updated}`}
          </span>
          {metrics && showReuseSummary && (
            <span className="text-sm text-navy-sub">
              <strong className="font-bold text-white">{plural(metrics.reuse_count, 'reuse', 'reuses')}</strong>
              {' · '}
              {plural(metrics.mention_count, 'mention', 'mentions')}
            </span>
          )}
          <span className="mt-1.5 flex flex-wrap gap-1.5 lg:justify-end">
            <MastTag>{ds.source}</MastTag>
            {licenses.map((l) => (
              <MastTag key={l.title} href={l.url ?? undefined} title={`License: ${l.title}`}>
                {l.label}
              </MastTag>
            ))}
            {ds.dataset_status === 'excluded' && (
              <MastTag tone="warn" title={`Excluded as junk (${ds.dataset_status_reason ?? 'unknown reason'}). Hidden from the dataset list and counts.`}>
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
        <span className="font-mono text-xs uppercase tracking-[0.08em] text-navy-muted">Dataset card</span>
        <div className="h-10 w-2/3 animate-skeleton rounded bg-white/20 motion-reduce:animate-none" />
        <div className="h-5 w-1/2 animate-skeleton rounded bg-white/15 motion-reduce:animate-none" />
      </div>
    </header>
  );
}

/* ───────────────────────── Band under the masthead ───────────────────────── */

function MetaBand({ data, primaryCount }: { data: DatasetDetailResponse; primaryCount: number }) {
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
    <div className="border-b border-line bg-surface">
      <div className="mx-auto flex max-w-[1440px] flex-col gap-2.5 px-4 py-3.5 sm:px-6 lg:px-8">
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
            <div className="flex items-center gap-1.5">
              <Eyebrow className="mr-0.5">Primary papers</Eyebrow>
              <Chip>{primaryCount}</Chip>
            </div>
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
      </div>
    </div>
  );
}

/* ───────────────────────── Cards ───────────────────────── */

function AboutCard({
  data,
  metrics,
  primaryCount,
}: {
  data: DatasetDetailResponse;
  metrics: DatasetMetrics | null;
  primaryCount: number;
}) {
  const ds = data.dataset;
  const tracked = metrics?.tracked ? metrics : null;
  const description = ds.full_description || ds.description || '';
  const licenses = licenseLinks(ds.license);
  const rows: Array<{ label: string; value: React.ReactNode }> = [];
  if (ds.num_subjects != null && ds.num_subjects > 0) {
    rows.push({ label: ds.source === 'OpenNeuro' ? 'Participants' : 'Subjects', value: ds.num_subjects.toLocaleString('en-US') });
  }
  rows.push({ label: 'Primary papers', value: primaryCount.toLocaleString('en-US') });
  if (tracked) {
    rows.push({ label: 'Citing papers found', value: tracked.coverage.citing_papers.toLocaleString('en-US') });
    rows.push({ label: 'Reuse papers', value: tracked.reuse_count.toLocaleString('en-US') });
  }
  if (licenses.length) {
    rows.push({
      label: 'License',
      value: (
        <span className="inline-flex flex-wrap justify-end gap-x-2">
          {licenses.map((l) =>
            l.url ? (
              <a key={l.title} href={l.url} target="_blank" rel="noopener noreferrer" title={l.title}>
                {l.label}
              </a>
            ) : (
              <span key={l.title} title={l.title}>
                {l.label}
              </span>
            ),
          )}
        </span>
      ),
    });
  }
  const published = formatPublishedDate(ds.created_at, ds.created_at_precision);
  if (published) rows.push({ label: 'Released', value: published });

  return (
    <Card aria-labelledby="about-h">
      <CardTitle id="about-h">About This Dataset</CardTitle>
      {description ? (
        <div className="prose prose-sm max-w-none break-words text-[15px] leading-[1.55] prose-p:my-0 prose-pre:border prose-pre:border-line-2 [&>*+*]:mt-3">
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
        <p className="m-0 break-words rounded-[5px] bg-panel px-3.5 py-3 text-[13px] leading-[1.55] text-ink-2">{citationText(ds)}</p>
      </div>
    </Card>
  );
}

const NOTE_LIST = 'm-0 flex list-disc flex-col gap-2 pl-[18px] text-sm leading-normal text-ink-2 marker:text-muted';

/** What the numbers say, then what to keep in mind about them. */
function NotesCard({ points, notes }: { points: string[]; notes: string[] }) {
  return (
    <Card aria-labelledby="notes-h">
      <CardTitle id="notes-h">Takeaways and Caveats</CardTitle>
      <SubTitle letter="A">Takeaways</SubTitle>
      {points.length ? (
        <ul className={NOTE_LIST}>
          {points.map((t) => (
            <li key={t}>{t}</li>
          ))}
        </ul>
      ) : (
        <p className="m-0 text-sm italic text-muted">Nothing to say until citing papers have been classified.</p>
      )}
      <SubTitle letter="B" className="mt-1.5">
        Caveats
      </SubTitle>
      <ul className={NOTE_LIST}>
        {notes.map((n) => (
          <li key={n}>{n}</li>
        ))}
      </ul>
    </Card>
  );
}

function ReuseTypesCard({
  papers,
  sameLabByDoi,
  reuseTotal,
  number,
}: {
  papers: DatasetPaperItem[];
  sameLabByDoi: Map<string, boolean>;
  /** Every reuse paper per /metrics, when loaded; the chart only sees the papers the page loaded. */
  reuseTotal: number | null;
  number: number;
}) {
  const rows = reuseTypeBreakdown(papers, sameLabByDoi);
  const shown = papers.filter((p) => p.label === 'REUSE').length;
  const max = Math.max(1, ...rows.map((r) => Math.max(r.independent, r.sameLab, r.unknown)));
  const independent = rows.reduce((n, r) => n + r.independent, 0);
  const sameLab = rows.reduce((n, r) => n + r.sameLab, 0);
  const unknown = rows.reduce((n, r) => n + r.unknown, 0);
  const bars: Array<{ key: 'independent' | 'sameLab' | 'unknown'; fill: string }> = [
    { key: 'independent', fill: 'bg-reuse' },
    { key: 'sameLab', fill: 'bg-reuse-soft' },
    ...(unknown > 0 ? [{ key: 'unknown' as const, fill: 'bg-bar' }] : []),
  ];
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
        {unknown > 0 && (
          <span className="flex items-center gap-1.5" title="No lab call yet; not counted as independent">
            <span className="h-2.5 w-3.5 bg-bar" aria-hidden="true" />
            lab unknown (n = {unknown})
          </span>
        )}
      </span>
      <div
        role="img"
        aria-label={`Reuse papers by type, different lab then same lab${unknown ? ' then lab unknown' : ''}: ${rows
          .map((r) => `${r.label} ${r.independent} and ${r.sameLab}${unknown ? ` and ${r.unknown}` : ''}`)
          .join(', ')}.`}
        className="flex flex-col gap-[7px]"
      >
        {rows.map((r) => (
          <div key={r.key} className="grid items-center gap-2" style={{ gridTemplateColumns: '118px minmax(0, 1fr)' }}>
            <span className="text-right text-[12.5px] text-ink-2">{r.label}</span>
            <span className="flex flex-col gap-[2px]">
              {bars.map((b) => (
                <span key={b.key} className="flex items-center gap-1">
                  <span className={cx('h-[9px]', b.fill)} style={{ width: `${(100 * r[b.key]) / max}%` }} />
                  <span className="text-[11px] tabular-nums text-ink-3">{r[b.key]}</span>
                </span>
              ))}
            </span>
          </div>
        ))}
      </div>
      <Note>
        Reuse papers by what they did with the data, as labelled from their full text. One paper counts once, by its
        strongest label.
        {reuseTotal !== null && reuseTotal > shown && (
          <>
            {' '}
            Showing {shown.toLocaleString('en-US')} of {plural(reuseTotal, 'reuse paper', 'reuse papers')}: the page loads
            the first {CITATION_LINKS_LOADED} citation links.
          </>
        )}
      </Note>
    </Card>
  );
}

/* ───────────────────────── Page ───────────────────────── */

export default function DatasetDetailPage() {
  const { source: sourceParam, datasetId: datasetIdParam } = useParams<{ source: string; datasetId: string }>();
  const source = decodedDatasetIdParam(sourceParam);
  const datasetId = decodedDatasetIdParam(datasetIdParam);
  const location = useLocation();
  const goBack = useGoBack();
  const [data, setData] = useState<DatasetDetailResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [metrics, setMetrics] = useState<DatasetMetrics | null>(null);
  const [metricsError, setMetricsError] = useState<string | null>(null);

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

  // Links made for the old Papers tab (#papers) open the card at the papers section.
  const scrolledToPapers = useRef(false);
  useEffect(() => {
    if (loading || !data || location.hash !== '#papers' || scrolledToPapers.current) return;
    scrolledToPapers.current = true;
    document.getElementById('papers')?.scrollIntoView();
  }, [loading, data, location.hash]);

  const trackedMetrics = metrics?.tracked ? metrics : null;
  const metricsTracked = metrics ? metrics.tracked : true;
  const papers = useMemo(() => (data ? buildPaperList(data.primary_papers, data.citations) : []), [data]);
  const sameLabByDoi = useMemo(
    () => new Map((trackedMetrics?.reuse_papers ?? []).map((p) => [p.doi, p.same_lab])),
    [trackedMetrics],
  );

  if (loading) {
    return (
      <div className="min-h-screen bg-canvas">
        <MastheadSkeleton />
        <div className="h-[68px] border-b border-line bg-surface" />
        <div className="mx-auto grid max-w-[1440px] grid-cols-1 gap-6 px-4 py-6 sm:px-6 lg:grid-cols-3 lg:px-8" aria-busy="true">
          {[0, 1, 2].map((i) => (
            <div key={i} className="h-72 animate-skeleton rounded-md border border-line bg-surface motion-reduce:animate-none" />
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
            <Button variant="secondary" onClick={goBack}>
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
  const points = takeaways(ds, trackedMetrics);
  const primaryCount = countByLabel(papers).PRIMARY;
  const hasReuse = papers.some((p) => p.label === 'REUSE');
  const impactLoading = !metrics && !metricsError;

  return (
    <div className="min-h-screen bg-canvas">
      <Masthead data={data} metrics={trackedMetrics} metricsTracked={metricsTracked} routeId={datasetId} />
      <MetaBand data={data} primaryCount={primaryCount} />

      <div
        className={cx(
          'mx-auto grid max-w-[1440px] grid-cols-1 items-start gap-6 px-4 py-6 sm:px-6 lg:px-8',
          'md:grid-cols-2 xl:grid-cols-[minmax(0,7fr)_minmax(0,5fr)_minmax(0,5fr)]',
        )}
      >
        <div className="flex min-w-0 flex-col gap-6">
          <AboutCard data={data} metrics={metrics} primaryCount={primaryCount} />
          <UseCard data={data} />
        </div>
        <div className="flex min-w-0 flex-col gap-6">
          <DatasetImpactCard
            number={1}
            metrics={trackedMetrics}
            loading={impactLoading}
            error={metricsError}
            untrackedNote={`Reuse metrics are not tracked for ${ds.source} yet: no citing papers have been mapped for this archive.`}
          />
          <DatasetPaperList number={2} items={papers} metrics={trackedMetrics} sameLabByDoi={sameLabByDoi} />
        </div>
        <div className="flex min-w-0 flex-col gap-6 md:col-span-2 md:grid md:grid-cols-2 md:gap-6 xl:col-span-1 xl:flex">
          {hasReuse && (
            <ReuseTypesCard papers={papers} sameLabByDoi={sameLabByDoi} reuseTotal={trackedMetrics?.reuse_count ?? null} number={3} />
          )}
          <NotesCard points={points} notes={notes} />
        </div>
      </div>
    </div>
  );
}
