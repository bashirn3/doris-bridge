const TASK_CONCURRENCY = 30;
const MS_PER_MONTH = 30.44 * 24 * 60 * 60 * 1000;
const THREE_MONTHS_MS = 3 * MS_PER_MONTH;
const LIVE_VEHICLE_VALIDATION = process.env.LIVE_VEHICLE_VALIDATION !== 'false';

const SITE_CALENDARS = [
  { siteId: 58, calendarId: 61 },
  { siteId: 59, calendarId: 62 },
  { siteId: 60, calendarId: 63 },
  { siteId: 61, calendarId: 64 },
];

function extractPhones(raw) {
  const matches = String(raw || '').match(/(?:\+?358|0)\d[\d\s-]{5,}/g) || [];
  return matches
    .map(m => m.replace(/[^0-9]/g, ''))
    .filter(n => n.length >= 8 && n.length <= 15)
    .filter(n => {
      // Mobile only: 04x or 050 prefixes (local) or 35840-35850 (international)
      if (n.startsWith('04') || n.startsWith('050')) return true;
      if (n.startsWith('35840') || n.startsWith('35841') || n.startsWith('35842') ||
          n.startsWith('35843') || n.startsWith('35844') || n.startsWith('35845') ||
          n.startsWith('35846') || n.startsWith('35847') || n.startsWith('35848') ||
          n.startsWith('35849') || n.startsWith('35850')) return true;
      return false;
    });
}

function normalizeRegistration(registration) {
  return String(registration || '').toUpperCase().replace(/\s+/g, '').trim();
}

function dateOnly(value) {
  if (!value) return null;
  const d = new Date(value);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().split('T')[0];
}

function findLiveVehicle(customer, registration) {
  const target = normalizeRegistration(registration);
  return (customer?.vehicles || []).find(v =>
    normalizeRegistration(v.registrationNumber || v.registerNumber || v.registration) === target
  );
}

async function validateLiveVehicle(kapa, customer, registration, now) {
  if (!LIVE_VEHICLE_VALIDATION) {
    return { ok: true, status: null, reason: 'disabled' };
  }

  if (!customer?.id) {
    return { ok: false, reason: 'live_vehicle_missing_customer_id' };
  }

  let liveCustomer;
  try {
    liveCustomer = await kapa.getCustomer(customer.id);
  } catch (err) {
    return { ok: false, reason: 'live_vehicle_lookup_failed', error: err.message };
  }

  const liveVehicle = findLiveVehicle(liveCustomer, registration);
  if (!liveVehicle) {
    return { ok: false, reason: 'live_vehicle_not_found' };
  }

  if (liveVehicle.valid?.end) {
    const validEnd = new Date(liveVehicle.valid.end);
    if (!isNaN(validEnd.getTime()) && validEnd.getTime() <= now.getTime()) {
      return { ok: false, reason: 'live_vehicle_inactive' };
    }
  }

  if (!liveVehicle.nextInspectionDate) {
    return { ok: false, reason: 'live_vehicle_missing_next_inspection_date', vehicle: liveVehicle };
  }

  const nextInspection = new Date(liveVehicle.nextInspectionDate);
  if (isNaN(nextInspection.getTime())) {
    return { ok: false, reason: 'live_vehicle_invalid_next_inspection_date', vehicle: liveVehicle };
  }

  const timeUntilDeadline = nextInspection.getTime() - now.getTime();
  if (timeUntilDeadline > THREE_MONTHS_MS) {
    return { ok: false, reason: 'live_vehicle_not_due', vehicle: liveVehicle };
  }

  return {
    ok: true,
    status: timeUntilDeadline < 0 ? 'passed' : 'due_soon',
    reason: timeUntilDeadline < 0 ? 'live_vehicle_overdue' : 'live_vehicle_due_soon',
    vehicle: liveVehicle,
    nextInspectionDate: nextInspection,
    daysUntilDeadline: Math.round(timeUntilDeadline / (24 * 60 * 60 * 1000)),
  };
}

function bumpStat(stats, key) {
  stats[key] = (stats[key] || 0) + 1;
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
    else if (ms >= 10) dueSoon.push(v);
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

// ── Helpers ──

function getNextInspectionDeadline(sale) {
  const tasks = sale?.tasks;
  if (!tasks || tasks.length === 0) return null;
  const vehicle = tasks[0]?.vehicle;
  if (!vehicle) return null;

  // Try ATJ base64 data for the authoritative next period end date
  if (vehicle.atjInfoBase64) {
    try {
      const atj = JSON.parse(Buffer.from(vehicle.atjInfoBase64, 'base64').toString());
      const tech = atj?.result?.sanoma?.ajoneuvo?.teknisettiedot;
      const nextEnd = tech?.seuraavamkajanloppupvm;
      if (nextEnd) {
        const d = new Date(nextEnd);
        if (!isNaN(d.getTime())) return d;
      }
    } catch {}
  }

  // Fallback: if nextInspectionBefore is in the future, use it directly
  if (vehicle.nextInspectionBefore) {
    const d = new Date(vehicle.nextInspectionBefore);
    if (!isNaN(d.getTime()) && d.getTime() > Date.now()) return d;
  }

  return null;
}

// ── Calendar-based due_soon scan ──
// Parallel station fetch, month-by-month with early exit, ATJ-verified.

async function scanDueSoonFromCalendar(kapa, { excludeNum, excludeCid, maxLeads, stats }) {
  const now = new Date();
  const msPerMonth = 30.44 * 24 * 60 * 60 * 1000;
  const THREE_MONTHS_MS = 3 * msPerMonth;

  // 9-11 months ago: annual-cycle cars whose deadline is 1-3 months from now
  const windowStart = new Date(now.getTime() - 11 * msPerMonth);
  const windowEnd = new Date(now.getTime() - 9 * msPerMonth);

  console.log(`[inspection-leads] Calendar DUE_SOON scan: ${windowStart.toISOString().split('T')[0]} to ${windowEnd.toISOString().split('T')[0]} (ATJ-verified, parallel)`);

  const leads = [];
  const seenPhones = new Set();
  const seenSales = new Set();
  const errors = [];
  let totalFetched = 0;
  let atjFiltered = 0;

  // Build 2-week chunks
  const chunkMs = 14 * 24 * 60 * 60 * 1000;
  const chunks = [];
  let cursor = new Date(windowStart);
  while (cursor < windowEnd) {
    const chunkEnd = new Date(Math.min(cursor.getTime() + chunkMs - 1, windowEnd.getTime()));
    chunks.push({ start: cursor.toISOString().split('T')[0], end: chunkEnd.toISOString().split('T')[0] });
    cursor = new Date(cursor.getTime() + chunkMs);
  }

  for (const chunk of chunks) {
    if (leads.length >= maxLeads) break;

    // Fetch all stations in parallel
    const stationResults = await Promise.all(
      SITE_CALENDARS.map(async ({ siteId, calendarId }) => {
        try {
          const data = await kapa.getCalendarEvents(calendarId, { start: chunk.start, end: chunk.end });
          const events = (data?.events || []).filter(
            e => e.eventType === 2 && e.info?.saleId && e.info?.registrationNumber
          );
          return events.map(e => ({ ...e, _siteId: siteId }));
        } catch (err) {
          errors.push({ calendarId, chunk: chunk.start, error: err.message });
          return [];
        }
      })
    );

    // Interleave across stations
    const chunkEvents = [];
    const maxLen = Math.max(...stationResults.map(r => r.length), 0);
    for (let idx = 0; idx < maxLen; idx++) {
      for (const events of stationResults) {
        if (idx < events.length && !seenSales.has(events[idx].info.saleId)) {
          seenSales.add(events[idx].info.saleId);
          chunkEvents.push(events[idx]);
        }
      }
    }

    if (chunkEvents.length === 0) continue;
    totalFetched += chunkEvents.length;

    // Process - fetch sale details in batches
    for (let i = 0; i < chunkEvents.length; i += TASK_CONCURRENCY) {
      if (leads.length >= maxLeads) break;

      const batch = chunkEvents.slice(i, i + TASK_CONCURRENCY);
      const saleResults = await Promise.all(
        batch.map(async (evt) => {
          try {
            const saleData = await kapa.getSale(evt._siteId, evt.info.saleId);
            return { evt, sale: saleData?.sale || saleData };
          } catch { return { evt, sale: null }; }
        })
      );

      for (const { evt, sale } of saleResults) {
        if (leads.length >= maxLeads) break;
        if (!sale?.customer) continue;

        // ATJ verification: only keep if deadline is in the future (0 to +3 months)
        const deadline = getNextInspectionDeadline(sale);
        if (deadline) {
          const timeUntilDeadline = deadline.getTime() - now.getTime();
          if (timeUntilDeadline < 0) { atjFiltered++; stats.not_due++; continue; }
          if (timeUntilDeadline > THREE_MONTHS_MS) { atjFiltered++; stats.not_due++; continue; }
        }

        const customer = sale.customer;
        if (customer.companyCustomer === true) { stats.business_skipped++; continue; }
        if (excludeCid.has(String(customer.id))) { stats.already_contacted++; continue; }

        const liveCheck = await validateLiveVehicle(kapa, customer, evt.info.registrationNumber, now);
        if (!liveCheck.ok) {
          bumpStat(stats, 'live_vehicle_skipped');
          bumpStat(stats, liveCheck.reason);
          continue;
        }
        if (liveCheck.status && liveCheck.status !== 'due_soon') {
          bumpStat(stats, 'live_vehicle_reclassified');
          continue;
        }

        const phones = extractPhones(customer.phone);
        if (phones.length === 0) { stats.no_phone++; continue; }
        const validPhone = phones.find(ph => !excludeNum.has(ph));
        if (!validPhone) { stats.already_contacted++; continue; }

        const normPhone = validPhone.startsWith('358') ? validPhone : '358' + validPhone.slice(1);
        if (seenPhones.has(normPhone)) continue;
        seenPhones.add(normPhone);

        const inspDate = new Date(evt.duration.start);
        const liveLastInspection = dateOnly(liveCheck.vehicle?.lastInspectedAt);
        const lastInspection = liveLastInspection || inspDate.toISOString().split('T')[0];
        const lastInspectionDate = new Date(lastInspection);
        const monthsSince = isNaN(lastInspectionDate.getTime())
          ? Math.round(((now - inspDate) / msPerMonth) * 10) / 10
          : Math.round(((now - lastInspectionDate) / msPerMonth) * 10) / 10;
        const daysUntilDeadline = liveCheck.daysUntilDeadline ??
          (deadline ? Math.round((deadline.getTime() - now.getTime()) / (24 * 60 * 60 * 1000)) : null);
        const nextInspectionBefore = liveCheck.nextInspectionDate
          ? dateOnly(liveCheck.nextInspectionDate)
          : deadline ? deadline.toISOString().split('T')[0] : null;
        const registration = normalizeRegistration(
          liveCheck.vehicle?.registrationNumber || evt.info.registrationNumber
        );

        leads.push({
          id: customer.id,
          phone: validPhone,
          firstName: (customer.firstName || '').trim(),
          name: (customer.name || ((customer.firstName || '') + ' ' + (customer.lastName || ''))).trim(),
          contactPerson: (customer.settings?.contactPerson || '').trim(),
          inspection_status: liveCheck.status || 'due_soon',
          months_since_inspection: monthsSince,
          days_until_deadline: daysUntilDeadline,
          next_inspection_before: nextInspectionBefore,
          vehicles: [{
            registration,
            manufacturer: '',
            vehicleClass: evt.info.vehicleClass || 'M1',
            lastInspection,
            stationId: evt._siteId,
            monthsSince,
          }],
          stationIds: [evt._siteId],
        });
      }
    }
  }

  console.log(`[inspection-leads] DUE_SOON done: ${leads.length} leads from ${totalFetched} inspections, ${atjFiltered} filtered by ATJ`);
  return { leads, fetched: totalFetched, errors };
}

// ── Calendar-based passed scan (oldest lapsed leads) ──
// Scans month-by-month from oldest, processes immediately, exits early once enough leads found.

async function scanPassedFromCalendar(kapa, { excludeNum, excludeCid, maxLeads, stats }) {
  const now = new Date();
  const msPerMonth = 30.44 * 24 * 60 * 60 * 1000;

  // 12-42 months ago, scan from oldest first (reaches back to mid-2022)
  const windowStart = new Date(now.getTime() - 42 * msPerMonth);
  const windowEnd = new Date(now.getTime() - 12 * msPerMonth);

  console.log(`[inspection-leads] Calendar PASSED scan: ${windowStart.toISOString().split('T')[0]} to ${windowEnd.toISOString().split('T')[0]} (month-by-month, early exit)`);

  const leads = [];
  const seenPhones = new Set();
  const seenSales = new Set();
  const errors = [];
  let totalFetched = 0;

  // Build monthly chunks (oldest first)
  const monthMs = 30.44 * 24 * 60 * 60 * 1000;
  const months = [];
  let cursor = new Date(windowStart);
  while (cursor < windowEnd) {
    const monthEnd = new Date(Math.min(cursor.getTime() + monthMs - 1, windowEnd.getTime()));
    months.push({ start: cursor.toISOString().split('T')[0], end: monthEnd.toISOString().split('T')[0] });
    cursor = new Date(cursor.getTime() + monthMs);
  }

  for (const month of months) {
    if (leads.length >= maxLeads) break;

    // Fetch all stations for this month in parallel
    const stationResults = await Promise.all(
      SITE_CALENDARS.map(async ({ siteId, calendarId }) => {
        try {
          const data = await kapa.getCalendarEvents(calendarId, { start: month.start, end: month.end });
          const events = (data?.events || []).filter(
            e => e.eventType === 2 && e.info?.saleId && e.info?.registrationNumber
          );
          return events.map(e => ({ ...e, _siteId: siteId }));
        } catch (err) {
          errors.push({ calendarId, month: month.start, error: err.message });
          return [];
        }
      })
    );

    // Interleave across stations (round-robin) so all stations get fair representation
    const monthEvents = [];
    const maxLen = Math.max(...stationResults.map(r => r.length));
    for (let idx = 0; idx < maxLen; idx++) {
      for (const events of stationResults) {
        if (idx < events.length && !seenSales.has(events[idx].info.saleId)) {
          seenSales.add(events[idx].info.saleId);
          monthEvents.push(events[idx]);
        }
      }
    }

    if (monthEvents.length === 0) continue;
    totalFetched += monthEvents.length;
    console.log(`[inspection-leads] PASSED month ${month.start}: ${monthEvents.length} unique inspections`);

    // Process this month's events - fetch sale details in batches
    for (let i = 0; i < monthEvents.length; i += TASK_CONCURRENCY) {
      if (leads.length >= maxLeads) break;

      const batch = monthEvents.slice(i, i + TASK_CONCURRENCY);
      const saleResults = await Promise.all(
        batch.map(async (evt) => {
          try {
            const saleData = await kapa.getSale(evt._siteId, evt.info.saleId);
            return { evt, sale: saleData?.sale || saleData };
          } catch { return { evt, sale: null }; }
        })
      );

      for (const { evt, sale } of saleResults) {
        if (leads.length >= maxLeads) break;
        if (!sale?.customer) continue;

        // ATJ verification: only keep if next deadline is in the past (truly overdue)
        const deadline = getNextInspectionDeadline(sale);
        if (deadline && deadline.getTime() > now.getTime()) { stats.not_due++; continue; }

        const customer = sale.customer;
        if (customer.companyCustomer === true) { stats.business_skipped++; continue; }
        if (excludeCid.has(String(customer.id))) { stats.already_contacted++; continue; }

        const liveCheck = await validateLiveVehicle(kapa, customer, evt.info.registrationNumber, now);
        if (!liveCheck.ok) {
          bumpStat(stats, 'live_vehicle_skipped');
          bumpStat(stats, liveCheck.reason);
          continue;
        }
        if (liveCheck.status && liveCheck.status !== 'passed') {
          bumpStat(stats, 'live_vehicle_reclassified');
          continue;
        }

        const phones = extractPhones(customer.phone);
        if (phones.length === 0) { stats.no_phone++; continue; }
        const validPhone = phones.find(ph => !excludeNum.has(ph));
        if (!validPhone) { stats.already_contacted++; continue; }

        const normPhone = validPhone.startsWith('358') ? validPhone : '358' + validPhone.slice(1);
        if (seenPhones.has(normPhone)) continue;
        seenPhones.add(normPhone);

        const inspDate = new Date(evt.duration.start);
        const liveLastInspection = dateOnly(liveCheck.vehicle?.lastInspectedAt);
        const lastInspection = liveLastInspection || inspDate.toISOString().split('T')[0];
        const lastInspectionDate = new Date(lastInspection);
        const monthsSince = isNaN(lastInspectionDate.getTime())
          ? Math.round(((now - inspDate) / msPerMonth) * 10) / 10
          : Math.round(((now - lastInspectionDate) / msPerMonth) * 10) / 10;
        const nextInspectionBefore = liveCheck.nextInspectionDate
          ? dateOnly(liveCheck.nextInspectionDate)
          : deadline ? deadline.toISOString().split('T')[0] : null;
        const registration = normalizeRegistration(
          liveCheck.vehicle?.registrationNumber || evt.info.registrationNumber
        );

        leads.push({
          id: customer.id,
          phone: validPhone,
          firstName: (customer.firstName || '').trim(),
          name: (customer.name || ((customer.firstName || '') + ' ' + (customer.lastName || ''))).trim(),
          contactPerson: (customer.settings?.contactPerson || '').trim(),
          inspection_status: liveCheck.status || 'passed',
          months_since_inspection: monthsSince,
          next_inspection_before: nextInspectionBefore,
          vehicles: [{
            registration,
            manufacturer: '',
            vehicleClass: evt.info.vehicleClass || 'M1',
            lastInspection,
            stationId: evt._siteId,
            monthsSince,
          }],
          stationIds: [evt._siteId],
        });
      }
    }
  }

  console.log(`[inspection-leads] PASSED done: ${leads.length} leads from ${totalFetched} inspections`);
  return { leads, fetched: totalFetched, errors };
}

// ── Main entry point ──

async function getInspectionLeads(kapa, {
  excludedNumbers = [],
  excludedCustomerIds = [],
  maxLeads = 50,
  defaultStationId = 58,
  pageLimit = 100,
  maxPages = 20,
  leadType = 'both',
} = {}) {
  const t0 = Date.now();

  const excludeNum = new Set();
  for (const n of excludedNumbers) {
    const s = String(n);
    excludeNum.add(s);
    if (s.startsWith('358')) excludeNum.add('0' + s.slice(3));
    else if (s.startsWith('0')) excludeNum.add('358' + s.slice(1));
  }

  const excludeCid = new Set(excludedCustomerIds.map(String));

  const stats = {
    no_phone: 0,
    already_contacted: 0,
    business_skipped: 0,
    not_due: 0,
    no_tasks: 0,
    live_vehicle_skipped: 0,
    live_vehicle_reclassified: 0,
    live_vehicle_missing_customer_id: 0,
    live_vehicle_lookup_failed: 0,
    live_vehicle_not_found: 0,
    live_vehicle_inactive: 0,
    live_vehicle_missing_next_inspection_date: 0,
    live_vehicle_invalid_next_inspection_date: 0,
    live_vehicle_not_due: 0,
    selected: 0,
  };

  let allPassed = [];
  let allDueSoon = [];
  let totalFetched = 0;
  let fetchErrors = [];

  if (leadType === 'due_soon') {
    console.log(`[inspection-leads] Using CALENDAR strategy for due_soon`);
    const result = await scanDueSoonFromCalendar(kapa, { excludeNum, excludeCid, maxLeads, stats });
    allDueSoon = result.leads;
    totalFetched = result.fetched;
    fetchErrors = result.errors;

  } else if (leadType === 'passed') {
    console.log(`[inspection-leads] Using CALENDAR strategy for passed (oldest lapsed)`);
    const result = await scanPassedFromCalendar(kapa, { excludeNum, excludeCid, maxLeads, stats });
    allPassed = result.leads;
    totalFetched = result.fetched;
    fetchErrors = result.errors;

  } else {
    console.log(`[inspection-leads] Using CALENDAR strategy for both: due_soon + passed`);
    const [dueSoonResult, passedResult] = await Promise.all([
      scanDueSoonFromCalendar(kapa, { excludeNum, excludeCid, maxLeads, stats }),
      scanPassedFromCalendar(kapa, { excludeNum, excludeCid, maxLeads, stats }),
    ]);

    allDueSoon = dueSoonResult.leads;
    totalFetched += dueSoonResult.fetched;
    fetchErrors.push(...dueSoonResult.errors);

    const seenIds = new Set(allDueSoon.map(l => l.id));
    const seenPhones = new Set(allDueSoon.map(l => {
      const p = l.phone;
      return p.startsWith('358') ? p : '358' + p.slice(1);
    }));

    for (const entry of passedResult.leads) {
      if (seenIds.has(entry.id)) continue;
      const normPhone = entry.phone.startsWith('358') ? entry.phone : '358' + entry.phone.slice(1);
      if (seenPhones.has(normPhone)) continue;
      seenIds.add(entry.id);
      seenPhones.add(normPhone);
      allPassed.push(entry);
    }
    totalFetched += passedResult.fetched;
    fetchErrors.push(...passedResult.errors);
  }

  let leads;
  if (leadType === 'passed') {
    leads = allPassed.slice(0, maxLeads);
  } else if (leadType === 'due_soon') {
    leads = allDueSoon.slice(0, maxLeads);
  } else {
    leads = [];
    let pi = 0, di = 0;
    while (leads.length < maxLeads && (pi < allPassed.length || di < allDueSoon.length)) {
      if (pi < allPassed.length) leads.push(allPassed[pi++]);
      if (leads.length >= maxLeads) break;
      if (di < allDueSoon.length) leads.push(allDueSoon[di++]);
    }
  }
  stats.selected = leads.length;

  const elapsed = Date.now() - t0;
  console.log(`[inspection-leads] Done: ${leads.length} leads (${allPassed.length} passed, ${allDueSoon.length} due_soon) from ${totalFetched} inspections/customers in ${elapsed}ms`);

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
