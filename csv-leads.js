require('dotenv').config();
const axios = require('axios');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY;
const DEFAULT_LEAD_SOURCE = (process.env.LEAD_SOURCE || '').toLowerCase();
const MAX_FETCH_LIMIT = 10000;
const CSV_DUE_SOON_WINDOW_DAYS = Number(process.env.CSV_DUE_SOON_WINDOW_DAYS || 90);
const CSV_MIN_MONTHS_SINCE_INSPECTION = Number(process.env.CSV_MIN_MONTHS_SINCE_INSPECTION || 8.5);
const CSV_MAX_MONTHS_SINCE_INSPECTION = Number(process.env.CSV_MAX_MONTHS_SINCE_INSPECTION || 14.5);

function hasSupabaseConfig() {
  return Boolean(SUPABASE_URL && SUPABASE_KEY);
}

function supabaseHeaders(extra = {}) {
  return {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    'Content-Type': 'application/json',
    ...extra,
  };
}

function supabaseUrl(path, params = {}) {
  const url = new URL(`/rest/v1/${path}`, SUPABASE_URL);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, String(value));
    }
  }
  return url.toString();
}

async function supabaseGet(path, params = {}) {
  if (!hasSupabaseConfig()) {
    throw new Error('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY/SUPABASE_KEY are required for CSV lead mode');
  }
  const { data } = await axios.get(supabaseUrl(path, params), {
    headers: supabaseHeaders(),
    timeout: 60000,
  });
  return data;
}

async function supabasePatch(path, params, body) {
  if (!hasSupabaseConfig()) {
    throw new Error('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY/SUPABASE_KEY are required for CSV lead mode');
  }
  const { data } = await axios.patch(supabaseUrl(path, params), body, {
    headers: supabaseHeaders({ Prefer: 'return=minimal' }),
    timeout: 60000,
  });
  return data;
}

// Best-effort: mark the rows we just handed out as contacted so the
// pending window slides forward and never clogs with already-sent leads.
// Failures are logged but never block lead delivery.
async function markCsvLeadsContacted(leadIds = []) {
  const ids = [...new Set(leadIds.map((id) => Number(id)).filter((id) => Number.isFinite(id)))];
  if (ids.length === 0) return;
  const now = new Date().toISOString();
  try {
    await supabasePatch(
      'tj_csv_leads',
      { id: `in.(${ids.join(',')})`, status: 'eq.pending' },
      { status: 'contacted', contacted_at: now, updated_at: now }
    );
    console.log(`[csv-leads] Marked ${ids.length} lead(s) as contacted`);
  } catch (err) {
    console.warn('[csv-leads] Failed to mark leads contacted:', err.response?.data?.message || err.message);
  }
}

async function getLeadSource() {
  if (DEFAULT_LEAD_SOURCE === 'csv' || DEFAULT_LEAD_SOURCE === 'doris') {
    return DEFAULT_LEAD_SOURCE;
  }
  if (!hasSupabaseConfig()) return 'doris';

  try {
    const rows = await supabaseGet('tj_config', {
      select: 'lead_source',
      id: 'eq.main',
      limit: 1,
    });
    const source = String(rows?.[0]?.lead_source || 'doris').toLowerCase();
    return source === 'csv' ? 'csv' : 'doris';
  } catch (err) {
    console.warn('[csv-leads] Could not read tj_config.lead_source, falling back to DORIS:', err.message);
    return 'doris';
  }
}

function normalizePhone(value = '') {
  const digits = String(value || '').replace(/[^0-9]/g, '');
  if (!digits) return '';
  if (digits.startsWith('0')) return `358${digits.slice(1)}`;
  return digits;
}

function phoneVariants(value = '') {
  const normalized = normalizePhone(value);
  const variants = new Set([String(value || '').replace(/[^0-9]/g, ''), normalized].filter(Boolean));
  if (normalized.startsWith('358')) variants.add(`0${normalized.slice(3)}`);
  return variants;
}

function buildExclusionSets(excludedNumbers = [], excludedCustomerIds = []) {
  const excludeNum = new Set();
  for (const number of excludedNumbers) {
    for (const variant of phoneVariants(number)) excludeNum.add(variant);
  }
  return {
    excludeNum,
    excludeCid: new Set(excludedCustomerIds.map(String).filter(Boolean)),
  };
}

function isExcluded(row, excludeNum, excludeCid) {
  if (row.doris_customer_id && excludeCid.has(String(row.doris_customer_id))) return true;
  for (const variant of phoneVariants(row.normalized_phone || row.phone)) {
    if (excludeNum.has(variant)) return true;
  }
  return false;
}

function dateOnly(value = new Date()) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return date.toISOString().slice(0, 10);
}

function addDays(date, days) {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function daysUntil(dateValue, from = new Date()) {
  if (!dateValue) return null;
  const target = new Date(`${dateValue}T00:00:00Z`);
  if (Number.isNaN(target.getTime())) return null;
  const base = new Date(`${dateOnly(from)}T00:00:00Z`);
  return Math.round((target.getTime() - base.getTime()) / (24 * 60 * 60 * 1000));
}

async function fetchCsvRows(leadType, limit) {
  // Safe CSV mode: the CSV dump ages quickly, so re-check deadlines at send
  // time and avoid stale/lapsed "passed" rows until live ATJ/DORIS validation
  // is available again.
  if (leadType === 'passed') return [];

  const today = new Date();
  const startDate = dateOnly(today);
  const endDate = dateOnly(addDays(today, CSV_DUE_SOON_WINDOW_DAYS));
  const params = {
    select: '*',
    status: 'eq.pending',
    lead_type: 'eq.due_soon',
    next_inspection_date: `gte.${startDate}`,
    months_since_inspection: `gte.${CSV_MIN_MONTHS_SINCE_INSPECTION}`,
    order: 'next_inspection_date.asc',
    limit: Math.min(Math.max(limit, 1), MAX_FETCH_LIMIT),
  };
  // PostgREST filters cannot repeat the same object key, so append the upper
  // bound manually below.
  const url = new URL(supabaseUrl('tj_csv_leads', params));
  url.searchParams.append('next_inspection_date', `lte.${endDate}`);
  url.searchParams.append('months_since_inspection', `lte.${CSV_MAX_MONTHS_SINCE_INSPECTION}`);
  url.searchParams.append('registration', 'not.is.null');
  url.searchParams.append('normalized_phone', 'not.is.null');
  if (leadType !== 'both' && leadType !== 'due_soon') return [];
  const rows = await axios.get(url.toString(), {
    headers: supabaseHeaders(),
    timeout: 60000,
  }).then((res) => res.data);
  return Array.isArray(rows) ? rows : [];
}

function csvRowToLead(row) {
  const stationIds = row.station_id ? [Number(row.station_id)] : [];
  const currentDaysUntilDeadline = daysUntil(row.next_inspection_date);
  return {
    id: row.id,
    source: 'csv_dump',
    source_batch_id: row.source_batch_id,
    csv_lead_id: row.id,
    phone: row.normalized_phone || normalizePhone(row.phone),
    firstName: row.first_name || '',
    name: row.name || [row.first_name, row.last_name].filter(Boolean).join(' ').trim(),
    contactPerson: '',
    inspection_status: row.lead_type,
    months_since_inspection: row.months_since_inspection === null ? null : Number(row.months_since_inspection),
    days_until_deadline: currentDaysUntilDeadline,
    next_inspection_before: row.next_inspection_date,
    vehicles: [{
      registration: row.registration,
      manufacturer: '',
      vehicleClass: row.vehicle_class || 'M1',
      lastInspection: row.last_inspection_date,
      stationId: row.station_id || null,
      monthsSince: row.months_since_inspection === null ? null : Number(row.months_since_inspection),
    }],
    stationIds,
    raw: {
      csv_lead_id: row.id,
      source_batch_id: row.source_batch_id,
      station_name: row.station_name,
      next_inspection_date: row.next_inspection_date,
    },
  };
}

function interleave(passed, dueSoon, maxLeads) {
  const leads = [];
  let pi = 0;
  let di = 0;
  while (leads.length < maxLeads && (pi < passed.length || di < dueSoon.length)) {
    if (pi < passed.length) leads.push(passed[pi++]);
    if (leads.length >= maxLeads) break;
    if (di < dueSoon.length) leads.push(dueSoon[di++]);
  }
  return leads;
}

async function getCsvInspectionLeads({
  excludedNumbers = [],
  excludedCustomerIds = [],
  maxLeads = 50,
  leadType = 'both',
} = {}) {
  const t0 = Date.now();
  const { excludeNum, excludeCid } = buildExclusionSets(excludedNumbers, excludedCustomerIds);
  const fetchLimit = Math.min(Math.max(maxLeads * 10, 100), MAX_FETCH_LIMIT);
  const stats = {
    already_contacted: 0,
    selected: 0,
    source: 'csv',
    safe_mode: {
      lead_type: 'due_soon_only',
      next_inspection_date: `today_to_${CSV_DUE_SOON_WINDOW_DAYS}_days`,
      months_since_inspection: `${CSV_MIN_MONTHS_SINCE_INSPECTION}_to_${CSV_MAX_MONTHS_SINCE_INSPECTION}`,
      passed_disabled: true,
    },
  };

  let dueSoonRows = [];
  let passedRows = [];
  if (leadType === 'due_soon') {
    dueSoonRows = await fetchCsvRows('due_soon', fetchLimit);
  } else if (leadType === 'passed') {
    passedRows = await fetchCsvRows('passed', fetchLimit);
  } else {
    [dueSoonRows, passedRows] = await Promise.all([
      fetchCsvRows('due_soon', fetchLimit),
      fetchCsvRows('passed', fetchLimit),
    ]);
  }

  const filterRows = (rows) => rows.filter((row) => {
    if (!isExcluded(row, excludeNum, excludeCid)) return true;
    stats.already_contacted++;
    return false;
  });

  const dueSoonLeads = filterRows(dueSoonRows).map(csvRowToLead);
  const passedLeads = filterRows(passedRows).map(csvRowToLead);

  let leads;
  if (leadType === 'due_soon') leads = dueSoonLeads.slice(0, maxLeads);
  else if (leadType === 'passed') leads = passedLeads.slice(0, maxLeads);
  else leads = interleave(passedLeads, dueSoonLeads, maxLeads);

  stats.selected = leads.length;

  // Mark the selected leads as contacted so subsequent fetches slide to fresh
  // pending rows instead of re-fetching the same earliest-by-date window.
  await markCsvLeadsContacted(leads.map((lead) => lead.csv_lead_id ?? lead.id));

  const elapsed = Date.now() - t0;
  console.log(`[csv-leads] Done: ${leads.length} leads (${passedLeads.length} passed, ${dueSoonLeads.length} due_soon) in ${elapsed}ms`);

  return {
    source: 'csv',
    leads,
    fetched_count: dueSoonRows.length + passedRows.length,
    selected_count: leads.length,
    passed_count: passedLeads.length,
    due_soon_count: dueSoonLeads.length,
    stats,
    fetch_errors: [],
    elapsed_ms: elapsed,
  };
}

async function getCsvLeadPoolSummary({
  excludedNumbers = [],
  excludedCustomerIds = [],
  leadType = 'due_soon',
} = {}) {
  if (leadType !== 'due_soon') {
    throw new Error('Only due_soon CSV lead pool summary is supported');
  }
  const { excludeNum, excludeCid } = buildExclusionSets(excludedNumbers, excludedCustomerIds);
  const rows = await fetchCsvRows('due_soon', MAX_FETCH_LIMIT);
  const activeRows = rows.filter((row) => !isExcluded(row, excludeNum, excludeCid));

  const stationCounts = {};
  const deadlineBuckets = { within_30_days: 0, days_31_to_60: 0, days_61_to_90: 0, unknown: 0 };
  for (const row of activeRows) {
    const station = row.station_name || 'Unknown';
    stationCounts[station] = (stationCounts[station] || 0) + 1;
    const days = daysUntil(row.next_inspection_date);
    if (days === null || days === undefined) deadlineBuckets.unknown++;
    else if (days <= 30) deadlineBuckets.within_30_days++;
    else if (days <= 60) deadlineBuckets.days_31_to_60++;
    else deadlineBuckets.days_61_to_90++;
  }

  return {
    status: 'done',
    generated_at: new Date().toISOString(),
    lead_type: leadType,
    method: 'csv_dump',
    estimate: false,
    source: 'csv',
    total_remaining: activeRows.length,
    station_counts: stationCounts,
    deadline_buckets: deadlineBuckets,
    excluded: {
      numbers: excludedNumbers.length,
      customer_ids: excludedCustomerIds.length,
    },
    scanned: {
      csv_due_soon_rows: rows.length,
    },
    skipped: {},
    calendar_errors: [],
    cache: { hit: false, cached_at: new Date().toISOString() },
  };
}

async function upsertCsvLeadRows(rows) {
  if (!hasSupabaseConfig()) {
    throw new Error('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY/SUPABASE_KEY are required to import CSV leads');
  }
  if (!rows.length) return;
  await axios.post(
    supabaseUrl('tj_csv_leads', { on_conflict: 'source_batch_id,source_key' }),
    rows,
    {
      headers: supabaseHeaders({ Prefer: 'resolution=merge-duplicates,return=minimal' }),
      timeout: 120000,
    }
  );
}

module.exports = {
  getLeadSource,
  getCsvInspectionLeads,
  getCsvLeadPoolSummary,
  markCsvLeadsContacted,
  normalizePhone,
  upsertCsvLeadRows,
};
