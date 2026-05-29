require('dotenv').config();
const axios = require('axios');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY;
const DEFAULT_LEAD_SOURCE = (process.env.LEAD_SOURCE || '').toLowerCase();
const MAX_FETCH_LIMIT = 10000;

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

async function fetchCsvRows(leadType, limit) {
  const params = {
    select: '*',
    status: 'eq.pending',
    order: 'next_inspection_date.asc',
    limit: Math.min(Math.max(limit, 1), MAX_FETCH_LIMIT),
  };
  if (leadType !== 'both') params.lead_type = `eq.${leadType}`;
  const rows = await supabaseGet('tj_csv_leads', params);
  return Array.isArray(rows) ? rows : [];
}

function csvRowToLead(row) {
  const stationIds = row.station_id ? [Number(row.station_id)] : [];
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
    days_until_deadline: row.days_until_deadline,
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
    const days = row.days_until_deadline;
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
  normalizePhone,
  upsertCsvLeadRows,
};
