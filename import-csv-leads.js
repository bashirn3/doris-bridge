#!/usr/bin/env node
require('dotenv').config();
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { normalizePhone, upsertCsvLeadRows } = require('./csv-leads');

const DEFAULT_FILE = 'kuluttaja-asiakkaat_ketju_28_05_2026.csv';
const DEFAULT_BATCH_ID = 'csv_dump_2026_05_28';
const CHUNK_SIZE = 500;

const STATION_IDS = [
  { id: 58, match: /vaajakoski/i, name: 'Vaajakoski' },
  { id: 59, match: /jämsä|jamsa/i, name: 'Jämsä' },
  { id: 60, match: /laukaa/i, name: 'Laukaa' },
  { id: 61, match: /muurame/i, name: 'Muurame' },
];

function argValue(name, fallback = '') {
  const prefix = `${name}=`;
  const found = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!found) return fallback;
  if (found === name) return 'true';
  return found.slice(prefix.length);
}

function parseCsvLine(line, delimiter = ';') {
  const cells = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const next = line[i + 1];
    if (char === '"' && inQuotes && next === '"') {
      current += '"';
      i++;
    } else if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === delimiter && !inQuotes) {
      cells.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  cells.push(current);
  return cells.map((value) => value.trim());
}

function parseDate(value) {
  const text = String(value || '').trim();
  const match = text.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (!match) return null;
  const [, dd, mm, yyyy] = match;
  const date = new Date(Date.UTC(Number(yyyy), Number(mm) - 1, Number(dd)));
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
}

function daysBetween(startDate, endDate) {
  const start = new Date(`${startDate}T00:00:00Z`);
  const end = new Date(`${endDate}T00:00:00Z`);
  return Math.round((end - start) / (24 * 60 * 60 * 1000));
}

function monthsBetween(startDate, endDate) {
  if (!startDate || !endDate) return null;
  return Math.round((daysBetween(startDate, endDate) / 30.44) * 10) / 10;
}

function isMobilePhone(phone) {
  const normalized = normalizePhone(phone);
  return /^(3584|35850)\d{6,}$/.test(normalized);
}

function cleanRegistration(value) {
  return String(value || '').toUpperCase().replace(/\s+/g, '').trim();
}

function stationFromText(value = '') {
  const text = String(value || '');
  const station = STATION_IDS.find((item) => item.match.test(text));
  return station || { id: null, name: text.replace(/^TJ-Katsastus Oy\s*\/\s*/i, '').trim() || '' };
}

function classifyLead({ phone, registration, nextInspectionDate, today }) {
  if (!phone) return { status: 'skipped', leadType: 'unknown', skipReason: 'missing_phone' };
  if (!isMobilePhone(phone)) return { status: 'skipped', leadType: 'unknown', skipReason: 'invalid_or_non_mobile_phone' };
  if (!registration) return { status: 'skipped', leadType: 'unknown', skipReason: 'missing_registration' };
  if (!nextInspectionDate) return { status: 'skipped', leadType: 'unknown', skipReason: 'missing_next_inspection_date' };

  const days = daysBetween(today, nextInspectionDate);
  if (days < 0) return { status: 'pending', leadType: 'passed', skipReason: '' };
  if (days <= 90) return { status: 'pending', leadType: 'due_soon', skipReason: '' };
  return { status: 'skipped', leadType: 'not_due', skipReason: 'next_inspection_more_than_90_days' };
}

function rowObject(cells) {
  return {
    Etunimi: cells[0] || '',
    Sukunimi: cells[1] || '',
    'Grano ID': cells[2] || '',
    Puhelin: cells[3] || '',
    'E-mail': cells[4] || '',
    Tila: cells[5] || '',
    'Lupa katsastusmuistutuksiin / E-mail': cells[6] || '',
    'Lupa katsastusmuistutuksiin / Tekstiviesti': cells[7] || '',
    'Markkinointilupa / E-mail': cells[8] || '',
    'Markkinointilupa / Tekstiviesti': cells[9] || '',
    Rekisterinumero: cells[10] || '',
    'Viimeisin katsastus': cells[11] || '',
    'Seuraava katsastusajankohta': cells[12] || '',
    Ajoneuvot: cells[13] || '',
    'Kutsu lähetetty': cells[14] || '',
    Kutsukanava: cells[15] || '',
    'Kutsuttu rekisteritunnus': cells[16] || '',
    Toimipaikat: cells[17] || '',
  };
}

function sourceKey(batchId, normalizedPhone, registration, nextInspectionDate, stationId, rowNumber) {
  const hash = crypto.createHash('sha1');
  hash.update(batchId);
  hash.update('|');
  hash.update(normalizedPhone || '');
  hash.update('|');
  hash.update(registration || '');
  hash.update('|');
  hash.update(nextInspectionDate || '');
  hash.update('|');
  hash.update(String(stationId || ''));
  hash.update('|');
  hash.update(String(rowNumber));
  return hash.digest('hex');
}

function mapRow(cells, rowNumber, { batchId, today }) {
  const raw = rowObject(cells);
  const firstName = raw.Etunimi.trim();
  const lastName = raw.Sukunimi.trim();
  const name = [firstName, lastName].filter(Boolean).join(' ').replace(/\s+/g, ' ').trim();
  const phone = raw.Puhelin.trim();
  const normalizedPhone = normalizePhone(phone);
  const registration = cleanRegistration(raw.Rekisterinumero);
  const lastInspectionDate = parseDate(raw['Viimeisin katsastus']);
  const nextInspectionDate = parseDate(raw['Seuraava katsastusajankohta']);
  const station = stationFromText(raw.Toimipaikat);
  const classification = classifyLead({ phone, registration, nextInspectionDate, today });
  const daysUntilDeadline = nextInspectionDate ? daysBetween(today, nextInspectionDate) : null;

  return {
    source_batch_id: batchId,
    source_key: sourceKey(batchId, normalizedPhone, registration, nextInspectionDate, station.id, rowNumber),
    status: classification.status,
    lead_type: classification.leadType,
    first_name: firstName,
    last_name: lastName,
    name,
    phone,
    normalized_phone: normalizedPhone,
    email: raw['E-mail'].trim(),
    doris_customer_id: raw['Grano ID'].trim() || null,
    customer_status: raw.Tila.trim(),
    registration,
    vehicles: raw.Ajoneuvot.trim(),
    vehicle_class: 'M1',
    station_id: station.id,
    station_name: station.name,
    last_inspection_date: lastInspectionDate,
    next_inspection_date: nextInspectionDate,
    days_until_deadline: daysUntilDeadline,
    months_since_inspection: monthsBetween(lastInspectionDate, today),
    reminder_email_at: parseDate(raw['Lupa katsastusmuistutuksiin / E-mail']),
    reminder_sms_at: parseDate(raw['Lupa katsastusmuistutuksiin / Tekstiviesti']),
    marketing_email_at: parseDate(raw['Markkinointilupa / E-mail']),
    marketing_sms_at: parseDate(raw['Markkinointilupa / Tekstiviesti']),
    invite_sent_at: parseDate(raw['Kutsu lähetetty']),
    invite_channel: raw.Kutsukanava.trim(),
    invited_registration: cleanRegistration(raw['Kutsuttu rekisteritunnus']),
    skip_reason: classification.skipReason,
    raw_row: raw,
    updated_at: new Date().toISOString(),
  };
}

function printStats(rows) {
  const counts = {};
  const stationCounts = {};
  for (const row of rows) {
    const key = `${row.status}:${row.lead_type}${row.skip_reason ? `:${row.skip_reason}` : ''}`;
    counts[key] = (counts[key] || 0) + 1;
    if (row.status === 'pending') {
      stationCounts[row.station_name || 'Unknown'] = (stationCounts[row.station_name || 'Unknown'] || 0) + 1;
    }
  }
  console.log('\nLead classification:');
  for (const [key, count] of Object.entries(counts).sort()) {
    console.log(`  ${key}: ${count}`);
  }
  console.log('\nPending leads by station:');
  for (const [station, count] of Object.entries(stationCounts).sort()) {
    console.log(`  ${station}: ${count}`);
  }
}

async function main() {
  const filePath = path.resolve(argValue('--file', DEFAULT_FILE));
  const batchId = argValue('--batch-id', DEFAULT_BATCH_ID);
  const dryRun = process.argv.includes('--dry-run');
  const today = argValue('--today', new Date().toISOString().slice(0, 10));

  const content = fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, '');
  const lines = content.split(/\r?\n/).filter((line) => line.trim());
  const dataLines = lines.slice(5);
  const rows = dataLines.map((line, index) => mapRow(parseCsvLine(line), index + 1, { batchId, today }));

  console.log(`File: ${filePath}`);
  console.log(`Batch: ${batchId}`);
  console.log(`Today/classification date: ${today}`);
  console.log(`Rows parsed: ${rows.length}`);
  printStats(rows);

  if (dryRun) {
    console.log('\nDry run only. No rows imported.');
    return;
  }

  for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
    const chunk = rows.slice(i, i + CHUNK_SIZE);
    await upsertCsvLeadRows(chunk);
    console.log(`Imported ${Math.min(i + CHUNK_SIZE, rows.length)} / ${rows.length}`);
  }
  console.log('\nImport complete.');
}

main().catch((err) => {
  console.error('Fatal:', err.response?.data || err.message);
  process.exit(1);
});
