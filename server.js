require('dotenv').config();
const crypto = require('crypto');
const express = require('express');
const { B2CAuth } = require('./b2c-auth');
const { DorisClient } = require('./doris-client');
const { KapaClient } = require('./kapa-client');
const { getInspectionLeads, getLeadPoolSummary } = require('./inspection-leads');

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '1mb' }));

const BRIDGE_API_KEY = process.env.BRIDGE_API_KEY;

function requireApiKey(req, res, next) {
  if (!BRIDGE_API_KEY) return next();
  if (req.headers['x-api-key'] === BRIDGE_API_KEY) return next();
  res.status(401).json({ error: 'Unauthorized – invalid or missing X-API-Key header' });
}

const auth = new B2CAuth();
const doris = new DorisClient(auth);
const kapa = new KapaClient(auth);

function wrap(fn) {
  return async (req, res) => {
    try {
      const result = await fn(req);
      res.json({ success: true, data: result });
    } catch (err) {
      console.error(`[Bridge] ${req.method} ${req.path} error:`, err.message);
      const status = err.response?.status || 500;
      res.status(status).json({
        success: false,
        error: err.message,
        detail: err.response?.data || null,
      });
    }
  };
}

// ── Health ──

app.get('/health', async (req, res) => {
  const hasSpa = !!auth.spaToken && Date.now() < auth.spaTokenExpiry;
  const hasKapa = !!auth.kapaSessionCookie && Date.now() < auth.kapaSessionExpiry;
  res.json({
    status: 'ok',
    auth: { spa: hasSpa, kapa: hasKapa },
    chainId: process.env.CHAIN_ID,
  });
});

// ── Sites ──

app.get('/api/doris/sites', requireApiKey, wrap(async () => {
  return doris.getSites();
}));

app.get('/api/doris/sites/:siteId', requireApiKey, wrap(async (req) => {
  return doris.getSite(parseInt(req.params.siteId));
}));

// ── Profile ──

app.get('/api/doris/profile', requireApiKey, wrap(async () => {
  return doris.getProfile();
}));

// ── Job Queue ──

app.get('/api/doris/sites/:siteId/jobs', requireApiKey, wrap(async (req) => {
  return doris.getJobQueue(parseInt(req.params.siteId));
}));

app.get('/api/doris/sites/:siteId/jobs/history', requireApiKey, wrap(async (req) => {
  return doris.getJobHistory(parseInt(req.params.siteId));
}));

app.get('/api/doris/sites/:siteId/jobs/stats', requireApiKey, wrap(async (req) => {
  return doris.getJobStatistics(parseInt(req.params.siteId));
}));

app.get('/api/doris/jobs/:jobId', requireApiKey, wrap(async (req) => {
  return doris.getJob(parseInt(req.params.jobId));
}));

app.post('/api/doris/jobs/annul', requireApiKey, wrap(async (req) => {
  return doris.annulJob(req.body);
}));

// ── Job Types ──

app.get('/api/doris/job-types', requireApiKey, wrap(async () => {
  return doris.getJobTypes();
}));

// ── Vehicle Data ──

app.get('/api/doris/vehicle-groups', requireApiKey, wrap(async () => {
  return doris.getVehicleGroups();
}));

app.get('/api/doris/vehicle-classes', requireApiKey, wrap(async () => {
  return doris.getVehicleClasses();
}));

// ── Inspection Lines ──

app.get('/api/doris/sites/:siteId/inspection-lines', requireApiKey, wrap(async (req) => {
  return doris.getInspectionLines(parseInt(req.params.siteId));
}));

// ── Users ──

app.get('/api/doris/sites/:siteId/users', requireApiKey, wrap(async (req) => {
  return doris.getUsersInSite(parseInt(req.params.siteId));
}));

// ═══════════════════════════════════════════════════
// KAPA ENDPOINTS (customers, calendar, booking)
// ═══════════════════════════════════════════════════

// ── Customers ──

app.get('/api/doris/customers', requireApiKey, wrap(async (req) => {
  const { q, offset, limit } = req.query;
  return kapa.searchCustomers(q || '', {
    offset: parseInt(offset) || 0,
    limit: parseInt(limit) || 20,
  });
}));

app.get('/api/doris/customers/:customerId', requireApiKey, wrap(async (req) => {
  return kapa.getCustomer(parseInt(req.params.customerId));
}));

app.post('/api/doris/sites/:siteId/customers', requireApiKey, wrap(async (req) => {
  return kapa.createCustomer(parseInt(req.params.siteId), req.body);
}));

app.put('/api/doris/sites/:siteId/customers/:customerId', requireApiKey, wrap(async (req) => {
  return kapa.updateCustomer(
    parseInt(req.params.siteId),
    parseInt(req.params.customerId),
    req.body
  );
}));

app.get('/api/doris/customers/:customerId/tasks', requireApiKey, wrap(async (req) => {
  return kapa.getCustomerTasks(parseInt(req.params.customerId));
}));

app.get('/api/doris/customers/:customerId/sale-events', requireApiKey, wrap(async (req) => {
  return kapa.getCustomerSaleEvents(parseInt(req.params.customerId));
}));

app.get('/api/doris/customers/export/:format', requireApiKey, wrap(async (req) => {
  return kapa.exportCustomers(req.params.format || 'csv');
}));

// ── Calendar / Booking ──

app.get('/api/doris/sites/:siteId/calendar/resources', requireApiKey, wrap(async (req) => {
  return kapa.getCalendarResources(parseInt(req.params.siteId));
}));

app.get('/api/doris/sites/:siteId/calendar/default-events', requireApiKey, wrap(async (req) => {
  return kapa.getDefaultEvents(parseInt(req.params.siteId));
}));

app.get('/api/doris/calendar/:calendarId/events', requireApiKey, wrap(async (req) => {
  const { start, end } = req.query;
  return kapa.getCalendarEvents(parseInt(req.params.calendarId), { start, end });
}));

app.get('/api/doris/calendar/:calendarId/capacity', requireApiKey, wrap(async (req) => {
  return kapa.getCalendarCapacity(parseInt(req.params.calendarId), req.query.date);
}));

app.get('/api/doris/calendar/:calendarId/resource/:resourceId/capacity', requireApiKey, wrap(async (req) => {
  return kapa.getResourceCapacity(
    parseInt(req.params.calendarId),
    parseInt(req.params.resourceId),
    req.query.date
  );
}));

app.get('/api/doris/sites/:siteId/availability', requireApiKey, wrap(async (req) => {
  const { date, resourceId } = req.query;
  if (!date) throw Object.assign(new Error('date query param required (YYYY-MM-DD)'), { response: { status: 400 } });
  return kapa.getAvailableSlots(parseInt(req.params.siteId), date, {
    resourceId: resourceId ? parseInt(resourceId) : undefined,
  });
}));

app.post('/api/doris/sites/:siteId/book', requireApiKey, wrap(async (req) => {
  return kapa.createBooking(parseInt(req.params.siteId), req.body);
}));

app.put('/api/doris/calendar/events/:eventId', requireApiKey, wrap(async (req) => {
  return kapa.updateCalendarEvent(parseInt(req.params.eventId), req.body);
}));

app.get('/api/doris/calendar/events/:eventId/log', requireApiKey, wrap(async (req) => {
  return kapa.getEventLog(parseInt(req.params.eventId));
}));

app.get('/api/doris/site-calendar-map', requireApiKey, wrap(async () => {
  return KapaClient.siteCalendarMap;
}));

// ── Reception / Sales ──

app.post('/api/doris/sites/:siteId/sales', requireApiKey, wrap(async (req) => {
  return kapa.createSale(parseInt(req.params.siteId), req.body);
}));

app.get('/api/doris/sites/:siteId/sales/:saleId', requireApiKey, wrap(async (req) => {
  return kapa.getSale(parseInt(req.params.siteId), parseInt(req.params.saleId));
}));

app.put('/api/doris/sites/:siteId/sales/:saleId', requireApiKey, wrap(async (req) => {
  return kapa.updateSale(
    parseInt(req.params.siteId),
    parseInt(req.params.saleId),
    req.body
  );
}));

app.delete('/api/doris/sites/:siteId/sales/:saleId', requireApiKey, wrap(async (req) => {
  return kapa.deleteSale(parseInt(req.params.siteId), parseInt(req.params.saleId));
}));

app.get('/api/doris/sites/:siteId/sales/search', requireApiKey, wrap(async (req) => {
  return kapa.searchSales(parseInt(req.params.siteId), req.query);
}));

app.get('/api/doris/sites/:siteId/sales/from-calendar/:eventId', requireApiKey, wrap(async (req) => {
  return kapa.getSaleFromCalendarEvent(
    parseInt(req.params.siteId),
    parseInt(req.params.eventId)
  );
}));

app.post('/api/doris/sites/:siteId/atj-search', requireApiKey, wrap(async (req) => {
  return kapa.atjSearch(parseInt(req.params.siteId), req.body);
}));

app.get('/api/doris/reception/options', requireApiKey, wrap(async () => {
  return kapa.getReceptionOptions();
}));

// ── Campaigns ──

app.get('/api/doris/campaigns', requireApiKey, wrap(async () => {
  return kapa.getCampaigns();
}));

app.post('/api/doris/campaigns', requireApiKey, wrap(async (req) => {
  return kapa.createCampaign(req.body);
}));

app.get('/api/doris/campaigns/:campaignId', requireApiKey, wrap(async (req) => {
  return kapa.getCampaign(parseInt(req.params.campaignId));
}));

app.put('/api/doris/campaigns/:campaignId', requireApiKey, wrap(async (req) => {
  return kapa.updateCampaign(parseInt(req.params.campaignId), req.body);
}));

app.get('/api/doris/sites/:siteId/campaigns', requireApiKey, wrap(async (req) => {
  return kapa.getCampaignsBySite(parseInt(req.params.siteId));
}));

// ── Tasks ──

app.get('/api/doris/sites/:siteId/tasks', requireApiKey, wrap(async (req) => {
  return kapa.getStationTasks(parseInt(req.params.siteId));
}));

// ── Products ──

app.get('/api/doris/sites/:siteId/products', requireApiKey, wrap(async (req) => {
  return kapa.getProducts(parseInt(req.params.siteId));
}));

app.get('/api/doris/products', requireApiKey, wrap(async () => {
  return kapa.getChainProducts();
}));

// ── Inspection Leads (async job pattern) ──

const jobs = new Map();
const leadPoolJobs = new Map();
const LEAD_POOL_JOB_TTL_MS = 12 * 60 * 60 * 1000;

app.post('/api/doris/inspection-leads', requireApiKey, (req, res) => {
  const {
    excluded_numbers = [],
    excluded_customer_ids = [],
    max_leads = 50,
    default_station_id = 58,
    page_limit = 100,
    max_pages = 20,
    lead_type = 'both',
  } = req.body || {};

  const jobId = Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  jobs.set(jobId, { status: 'running', startedAt: Date.now() });

  getInspectionLeads(kapa, {
    excludedNumbers: excluded_numbers,
    excludedCustomerIds: excluded_customer_ids,
    maxLeads: max_leads,
    defaultStationId: default_station_id,
    pageLimit: page_limit,
    maxPages: max_pages,
    leadType: lead_type,
  }).then(result => {
    jobs.set(jobId, { status: 'done', result, finishedAt: Date.now() });
    setTimeout(() => jobs.delete(jobId), 10 * 60 * 1000);
  }).catch(err => {
    jobs.set(jobId, { status: 'error', error: err.message, finishedAt: Date.now() });
    setTimeout(() => jobs.delete(jobId), 10 * 60 * 1000);
  });

  res.json({ success: true, data: { job_id: jobId, status: 'running' } });
});

app.get('/api/doris/inspection-leads/status/:jobId', requireApiKey, (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ success: false, error: 'Job not found or expired' });
  if (job.status === 'running') {
    return res.json({ success: true, data: { status: 'running', elapsed_ms: Date.now() - job.startedAt } });
  }
  if (job.status === 'error') {
    return res.json({ success: false, error: job.error });
  }
  res.json({ success: true, data: { status: 'done', ...job.result } });
});

app.post('/api/doris/lead-pool/summary', requireApiKey, (req, res) => {
  const {
    excluded_numbers = [],
    excluded_customer_ids = [],
    lead_type = 'due_soon',
    refresh = false,
  } = req.body || {};

  const key = leadPoolJobKey(lead_type, excluded_numbers, excluded_customer_ids);
  const existing = leadPoolJobs.get(key);
  if (!refresh && existing?.status === 'done' && Date.now() - existing.finishedAt < LEAD_POOL_JOB_TTL_MS) {
    return res.json({ success: true, data: { status: 'done', ...existing.result } });
  }
  if (existing?.status === 'running') {
    return res.json({
      success: true,
      data: {
        status: 'running',
        lead_type,
        started_at: new Date(existing.startedAt).toISOString(),
        elapsed_ms: Date.now() - existing.startedAt,
      },
    });
  }
  if (existing?.status === 'error' && Date.now() - existing.finishedAt < 10 * 60 * 1000) {
    return res.json({ success: false, error: existing.error });
  }

  const startedAt = Date.now();
  leadPoolJobs.set(key, { status: 'running', startedAt });

  getLeadPoolSummary(kapa, {
    excludedNumbers: excluded_numbers,
    excludedCustomerIds: excluded_customer_ids,
    leadType: lead_type,
    refresh: true,
  }).then((result) => {
    leadPoolJobs.set(key, { status: 'done', result, finishedAt: Date.now() });
    setTimeout(() => leadPoolJobs.delete(key), LEAD_POOL_JOB_TTL_MS);
  }).catch((err) => {
    leadPoolJobs.set(key, { status: 'error', error: err.message, finishedAt: Date.now() });
    setTimeout(() => leadPoolJobs.delete(key), 10 * 60 * 1000);
  });

  res.json({
    success: true,
    data: {
      status: 'running',
      lead_type,
      started_at: new Date(startedAt).toISOString(),
      elapsed_ms: 0,
    },
  });
});

function leadPoolJobKey(leadType, excludedNumbers, excludedCustomerIds) {
  const today = new Date().toISOString().slice(0, 10);
  return crypto
    .createHash('sha1')
    .update(`${leadType}|${today}|${excludedNumbers.map(String).sort().join(',')}|${excludedCustomerIds.map(String).sort().join(',')}`)
    .digest('hex');
}

// ── Metadata (convenience for n8n) ──

app.get('/api/doris/metadata', requireApiKey, wrap(async () => {
  const [sites, jobTypes, vehicleClasses] = await Promise.all([
    doris.getSites(),
    doris.getJobTypes(),
    doris.getVehicleClasses(),
  ]);
  return { sites, jobTypes, vehicleClasses, chainId: process.env.CHAIN_ID };
}));

// ──────────────────────────────────────────────────

const PORT = process.env.PORT || 3457;

app.listen(PORT, () => {
  console.log(`[DORIS Bridge] http://localhost:${PORT}`);
  console.log();
  console.log('  Health:');
  console.log('    GET  /health');
  console.log();
  console.log('  Azure Functions (DORIS):');
  console.log('    GET  /api/doris/sites');
  console.log('    GET  /api/doris/sites/:siteId/jobs');
  console.log('    GET  /api/doris/job-types');
  console.log('    GET  /api/doris/metadata');
  console.log();
  console.log('  KAPA (Customers):');
  console.log('    GET  /api/doris/customers?q=search&offset=0&limit=20');
  console.log('    GET  /api/doris/customers/:id');
  console.log('    GET  /api/doris/customers/:id/tasks');
  console.log();
  console.log('  KAPA (Calendar/Booking):');
  console.log('    GET  /api/doris/sites/:siteId/calendar/resources');
  console.log('    GET  /api/doris/sites/:siteId/availability?date=YYYY-MM-DD');
  console.log('    POST /api/doris/sites/:siteId/book');
  console.log('    PUT  /api/doris/calendar/events/:eventId');
  console.log('    GET  /api/doris/site-calendar-map');
  console.log();
  console.log('  KAPA (Sales/Reception):');
  console.log('    POST /api/doris/sites/:siteId/sales');
  console.log('    GET  /api/doris/sites/:siteId/sales/:saleId');
  console.log();
  console.log('  KAPA (Campaigns):');
  console.log('    GET  /api/doris/campaigns');
  console.log('    POST /api/doris/campaigns');
  console.log();
  console.log('  Inspection Leads:');
  console.log('    POST /api/doris/inspection-leads');
  console.log();
});
