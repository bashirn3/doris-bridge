const TASK_CONCURRENCY = 15;

function extractPhones(raw) {
  const matches = String(raw || '').match(/(?:\+?358|0)\d[\d\s-]{5,}/g) || [];
  return matches
    .map(m => m.replace(/[^0-9]/g, ''))
    .filter(n => n.length >= 8 && n.length <= 15);
}

function classifyInspection(tasks) {
  if (!tasks || tasks.length === 0) {
    return { status: 'unknown', monthsSince: 0, vehicles: [] };
  }

  const now = Date.now();
  const byReg = {};

  for (const t of tasks) {
    const reg = t.registerNumber;
    if (!reg) continue;
    const start = t.duration?.start ? new Date(t.duration.start) : null;
    if (!start || isNaN(start.getTime())) continue;

    if (!byReg[reg] || start > byReg[reg]._date) {
      const make = t.manufacturer?.name || '';
      byReg[reg] = {
        registration: reg,
        manufacturer: make.toLowerCase() === 'merkki' ? '' : make,
        vehicleClass: t.vehicleClass?.id || '',
        lastInspection: start.toISOString().split('T')[0],
        stationId: t.stationId || null,
        _date: start,
      };
    }
  }

  const vehicles = Object.values(byReg);
  if (vehicles.length === 0) {
    return { status: 'unknown', monthsSince: 0, vehicles: [] };
  }

  const overdue = [];
  const dueSoon = [];

  for (const v of vehicles) {
    const ms = (now - v._date.getTime()) / (30.44 * 24 * 60 * 60 * 1000);
    v.monthsSince = Math.round(ms * 10) / 10;
    delete v._date;
    if (ms >= 12) overdue.push(v);
    else if (ms >= 11) dueSoon.push(v);
  }

  const actionable = overdue.length > 0 ? overdue : dueSoon;
  const status = overdue.length > 0 ? 'passed'
    : dueSoon.length > 0 ? 'due_soon'
    : 'not_due';
  const maxMonths = actionable.reduce((m, v) => Math.max(m, v.monthsSince || 0), 0);

  return { status, monthsSince: maxMonths, vehicles: actionable };
}

function buildEntry(c, phone, inspection, defaultStationId) {
  return {
    id: c.id,
    phone,
    firstName: c.firstName || '',
    name: c.name || ((c.firstName || '') + ' ' + (c.lastName || '')).trim(),
    contactPerson: c.settings?.contactPerson || '',
    inspection_status: inspection.status,
    months_since_inspection: inspection.monthsSince,
    vehicles: inspection.vehicles,
    stationIds: Array.isArray(c.stationIds) && c.stationIds.length > 0
      ? c.stationIds
      : [defaultStationId],
  };
}

async function scanFromOffset(kapa, startOffset, {
  excludeNum, excludeCid, target, defaultStationId, pageLimit, maxScanPages, stats,
}) {
  const passed = [];
  const dueSoon = [];
  const errors = [];
  let fetched = 0;

  for (let p = 0; p < maxScanPages; p++) {
    if (passed.length >= target && dueSoon.length >= target) break;

    const offset = startOffset + p * pageLimit;
    let customers;
    try {
      const page = await kapa.searchCustomers('', { offset, limit: pageLimit });
      customers = page?.customers || [];
      fetched += customers.length;
    } catch (e) {
      errors.push({ offset, error: e.message });
      break;
    }
    if (customers.length === 0) break;

    const candidates = [];
    for (const c of customers) {
      if (c.companyCustomer === true) { stats.business_skipped++; continue; }
      if (excludeCid.has(String(c.id))) { stats.already_contacted++; continue; }
      const phones = extractPhones(c.phone);
      if (phones.length === 0) { stats.no_phone++; continue; }
      const validPhone = phones.find(ph => !excludeNum.has(ph));
      if (!validPhone) { stats.already_contacted++; continue; }
      candidates.push({ customer: c, phone: validPhone });
    }

    for (let i = 0; i < candidates.length; i += TASK_CONCURRENCY) {
      const batch = candidates.slice(i, i + TASK_CONCURRENCY);
      const results = await Promise.all(
        batch.map(async ({ customer }) => {
          try {
            const tasks = await kapa.getCustomerTasks(customer.id);
            return Array.isArray(tasks) ? tasks : [];
          } catch { return []; }
        })
      );
      for (let j = 0; j < batch.length; j++) {
        const inspection = classifyInspection(results[j]);
        if (inspection.status === 'not_due') { stats.not_due++; continue; }
        if (inspection.status === 'unknown') { stats.no_tasks++; continue; }
        const entry = buildEntry(batch[j].customer, batch[j].phone, inspection, defaultStationId);
        if (inspection.status === 'passed') passed.push(entry);
        else dueSoon.push(entry);
      }
    }

    if (customers.length < pageLimit) break;
  }

  return { passed, dueSoon, fetched, errors };
}

async function getInspectionLeads(kapa, {
  excludedNumbers = [],
  excludedCustomerIds = [],
  maxLeads = 50,
  defaultStationId = 58,
  pageLimit = 100,
  maxPages = 20,
} = {}) {
  const t0 = Date.now();
  const excludeNum = new Set(excludedNumbers.map(String));
  const excludeCid = new Set(excludedCustomerIds.map(String));
  const halfTarget = Math.ceil(maxLeads / 2);

  const stats = {
    no_phone: 0,
    already_contacted: 0,
    business_skipped: 0,
    not_due: 0,
    no_tasks: 0,
    selected: 0,
  };

  const shared = { excludeNum, excludeCid, target: halfTarget, defaultStationId, pageLimit, maxScanPages: maxPages, stats };

  console.log(`[inspection-leads] Dual scan: offset 0 + offset 20000, target ${halfTarget} per pool`);

  const [oldResult, newResult] = await Promise.all([
    scanFromOffset(kapa, 0, shared),
    scanFromOffset(kapa, 20000, shared),
  ]);

  const seenIds = new Set();
  const allPassed = [];
  const allDueSoon = [];

  for (const entry of [...oldResult.passed, ...newResult.passed]) {
    if (seenIds.has(entry.id)) continue;
    seenIds.add(entry.id);
    allPassed.push(entry);
  }
  for (const entry of [...oldResult.dueSoon, ...newResult.dueSoon]) {
    if (seenIds.has(entry.id)) continue;
    seenIds.add(entry.id);
    allDueSoon.push(entry);
  }

  allPassed.sort((a, b) => (b.months_since_inspection || 0) - (a.months_since_inspection || 0));
  allDueSoon.sort((a, b) => (b.months_since_inspection || 0) - (a.months_since_inspection || 0));

  const leads = [];
  let pi = 0, di = 0;
  while (leads.length < maxLeads && (pi < allPassed.length || di < allDueSoon.length)) {
    if (pi < allPassed.length) leads.push(allPassed[pi++]);
    if (leads.length >= maxLeads) break;
    if (di < allDueSoon.length) leads.push(allDueSoon[di++]);
  }
  stats.selected = leads.length;

  const totalFetched = oldResult.fetched + newResult.fetched;
  const fetchErrors = [...oldResult.errors, ...newResult.errors];
  const elapsed = Date.now() - t0;

  console.log(`[inspection-leads] Done: ${leads.length} leads (${allPassed.length} passed, ${allDueSoon.length} due_soon) from ${totalFetched} customers in ${elapsed}ms`);
  console.log(`[inspection-leads] Old scan: ${oldResult.fetched} fetched (${oldResult.passed.length}p/${oldResult.dueSoon.length}d), New scan: ${newResult.fetched} fetched (${newResult.passed.length}p/${newResult.dueSoon.length}d)`);

  return {
    leads,
    fetched_count: totalFetched,
    selected_count: leads.length,
    passed_count: allPassed.length,
    due_soon_count: allDueSoon.length,
    stats,
    fetch_errors: fetchErrors,
    elapsed_ms: elapsed,
  };
}

module.exports = { getInspectionLeads };
