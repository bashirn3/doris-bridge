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

async function getInspectionLeads(kapa, {
  excludedNumbers = [],
  excludedCustomerIds = [],
  maxLeads = 50,
  defaultStationId = 58,
  pageLimit = 100,
  maxPages = 10,
} = {}) {
  const t0 = Date.now();
  const excludeNum = new Set(excludedNumbers.map(String));
  const excludeCid = new Set(excludedCustomerIds.map(String));

  const allCustomers = [];
  const fetchErrors = [];

  for (let p = 0; p < maxPages; p++) {
    const offset = p * pageLimit;
    try {
      const page = await kapa.searchCustomers('', { offset, limit: pageLimit });
      const customers = page?.customers || [];
      allCustomers.push(...customers);
      if (customers.length < pageLimit) break;
    } catch (e) {
      fetchErrors.push({ offset, error: e.message });
      break;
    }
  }

  console.log(`[inspection-leads] Fetched ${allCustomers.length} customers in ${Date.now() - t0}ms`);

  const candidates = [];
  const stats = {
    no_phone: 0,
    already_contacted: 0,
    business_skipped: 0,
    not_due: 0,
    no_tasks: 0,
    selected: 0,
  };

  for (const c of allCustomers) {
    if (c.companyCustomer === true) {
      stats.business_skipped++;
      continue;
    }

    if (excludeCid.has(String(c.id))) {
      stats.already_contacted++;
      continue;
    }

    const phones = extractPhones(c.phone);
    if (phones.length === 0) {
      stats.no_phone++;
      continue;
    }

    const validPhone = phones.find(p => !excludeNum.has(p));
    if (!validPhone) {
      stats.already_contacted++;
      continue;
    }

    candidates.push({ customer: c, phone: validPhone });
  }

  console.log(`[inspection-leads] ${candidates.length} candidates after pre-filter (${Date.now() - t0}ms)`);

  for (let i = 0; i < candidates.length; i += TASK_CONCURRENCY) {
    const batch = candidates.slice(i, i + TASK_CONCURRENCY);
    const results = await Promise.all(
      batch.map(async ({ customer }) => {
        try {
          const tasks = await kapa.getCustomerTasks(customer.id);
          return Array.isArray(tasks) ? tasks : [];
        } catch {
          return [];
        }
      })
    );
    for (let j = 0; j < batch.length; j++) {
      batch[j].tasks = results[j];
    }
  }

  console.log(`[inspection-leads] Tasks fetched for ${candidates.length} candidates (${Date.now() - t0}ms)`);

  const passed = [];
  const dueSoonList = [];

  for (const { customer: c, phone, tasks } of candidates) {
    const inspection = classifyInspection(tasks);

    if (inspection.status === 'not_due') { stats.not_due++; continue; }
    if (inspection.status === 'unknown') { stats.no_tasks++; continue; }

    const entry = {
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

    if (inspection.status === 'passed') passed.push(entry);
    else dueSoonList.push(entry);
  }

  passed.sort((a, b) => (b.months_since_inspection || 0) - (a.months_since_inspection || 0));
  dueSoonList.sort((a, b) => (b.months_since_inspection || 0) - (a.months_since_inspection || 0));

  const leads = [];
  let pi = 0, di = 0;
  while (leads.length < maxLeads && (pi < passed.length || di < dueSoonList.length)) {
    if (pi < passed.length) leads.push(passed[pi++]);
    if (leads.length >= maxLeads) break;
    if (di < dueSoonList.length) leads.push(dueSoonList[di++]);
  }
  stats.selected = leads.length;

  const elapsed = Date.now() - t0;
  console.log(`[inspection-leads] Done: ${leads.length} leads (${passed.length} passed, ${dueSoonList.length} due_soon) in ${elapsed}ms`);

  return {
    leads,
    fetched_count: allCustomers.length,
    selected_count: leads.length,
    passed_count: passed.length,
    due_soon_count: dueSoonList.length,
    stats,
    fetch_errors: fetchErrors,
    elapsed_ms: elapsed,
  };
}

module.exports = { getInspectionLeads };
