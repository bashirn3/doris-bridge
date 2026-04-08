require('dotenv').config();
const axios = require('axios');

const KAPA_BASE = process.env.KAPA_BASE_URL;
const CHAIN_ID = process.env.CHAIN_ID;

const SITE_CALENDAR_MAP = {
  58: { calendarId: 61, name: 'Vaajakoski', onlineResource: 104 },
  59: { calendarId: 62, name: 'Jämsä',      onlineResource: 105 },
  60: { calendarId: 63, name: 'Laukaa',      onlineResource: 106 },
  61: { calendarId: 64, name: 'Muurame',     onlineResource: 427 },
};

const DEFAULT_PRODUCT_ID = 3214;
const DEFAULT_WORK_TYPE_ID = 347;
const SLOT_DURATION_MIN = 15;

class KapaClient {
  constructor(auth) {
    this.auth = auth;
    this.http = axios.create({
      baseURL: KAPA_BASE,
      timeout: 30000,
      headers: {
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
      },
    });
  }

  async _headers() {
    const session = await this.auth.getKapaSession();
    return { Cookie: session };
  }

  async _get(path, params = {}) {
    const headers = await this._headers();
    const res = await this.http.get(path, { headers, params });
    return res.data;
  }

  async _post(path, data = {}) {
    const headers = await this._headers();
    const res = await this.http.post(path, data, {
      headers: { ...headers, 'Content-Type': 'application/json' },
    });
    return res.data;
  }

  async _put(path, data = {}) {
    const headers = await this._headers();
    const res = await this.http.put(path, data, {
      headers: { ...headers, 'Content-Type': 'application/json' },
    });
    return res.data;
  }

  async _delete(path) {
    const headers = await this._headers();
    const res = await this.http.delete(path, { headers });
    return res.data;
  }

  _siteCalendar(siteId) {
    const mapping = SITE_CALENDAR_MAP[siteId];
    if (!mapping) throw new Error(`Unknown site ID ${siteId}. Valid: ${Object.keys(SITE_CALENDAR_MAP).join(', ')}`);
    return mapping;
  }

  // ── Customers ──

  async searchCustomers(query = '', { offset = 0, limit = 20 } = {}) {
    return this._get(`/chains/${CHAIN_ID}/customers`, {
      search: query, offset, limit,
    });
  }

  async searchCustomersBySite(stationId, query = '', { offset = 0, limit = 20 } = {}) {
    return this._get(`/chains/${CHAIN_ID}/stations/${stationId}/customers`, {
      search: query, offset, limit,
    });
  }

  async getCustomer(customerId) {
    return this._get(`/chains/${CHAIN_ID}/customers/${customerId}`);
  }

  async createCustomer(stationId, data) {
    return this._post(`/chains/${CHAIN_ID}/stations/${stationId}/customers`, data);
  }

  async updateCustomer(stationId, customerId, data) {
    return this._put(`/chains/${CHAIN_ID}/stations/${stationId}/customers/${customerId}`, data);
  }

  async getCustomerTasks(customerId) {
    return this._get(`/chains/${CHAIN_ID}/customers/${customerId}/tasks`);
  }

  async getCustomerSaleEvents(customerId) {
    return this._get(`/chains/${CHAIN_ID}/customers/${customerId}/sale_events`);
  }

  async updateCustomerVehicles(customerId, vehicles) {
    return this._put(`/chains/${CHAIN_ID}/customers/${customerId}/vehicles`, vehicles);
  }

  async exportCustomers(format = 'csv') {
    const path = format === 'csv'
      ? `/chains/${CHAIN_ID}/customers/export/csv`
      : `/chains/${CHAIN_ID}/customers/export`;
    return this._get(path);
  }

  // ── Calendar / Booking ──

  async getCalendarEvents(calendarId, { start, end } = {}) {
    const params = {};
    if (start) params.start = start;
    if (end) params.end = end;
    return this._get(`/calendar/${calendarId}/events`, params);
  }

  async getCalendarCapacity(calendarId, date) {
    const params = {};
    if (date) params.date = date;
    return this._get(`/calendar/${calendarId}/capacity`, params);
  }

  async getResourceCapacity(calendarId, resourceId, date) {
    const params = {};
    if (date) params.date = date;
    return this._get(`/calendar/${calendarId}/resource/${resourceId}/capacity`, params);
  }

  async getCalendarResources(stationId) {
    return this._get(`/calendar/${CHAIN_ID}/${stationId}/resources`);
  }

  async getDefaultEvents(stationId) {
    return this._get(`/calendar/${CHAIN_ID}/${stationId}/default-events`);
  }

  /**
   * Compute available 15-minute slots for a site on a given date.
   *
   * The KAPA capacity endpoint only returns open/close transitions (e.g.
   * capacity=1 at 09:00, capacity=0 at 17:00).  We expand those into
   * individual 15-min windows and subtract existing booking events.
   */
  async getAvailableSlots(siteId, date, { resourceId } = {}) {
    const mapping = this._siteCalendar(siteId);
    const calId = mapping.calendarId;
    const targetResource = resourceId || mapping.onlineResource;

    const [capacityData, eventsData] = await Promise.all([
      this.getResourceCapacity(calId, targetResource, date),
      this.getCalendarEvents(calId, { start: date, end: date }),
    ]);

    const caps = capacityData.capacities || [];
    let openStart = null;
    const windows = [];
    for (const c of caps) {
      if (c.capacity > 0 && !openStart) {
        openStart = new Date(c.time);
      } else if (c.capacity === 0 && openStart) {
        windows.push({ from: openStart, to: new Date(c.time) });
        openStart = null;
      }
    }

    const allSlots = [];
    for (const w of windows) {
      let t = new Date(w.from);
      while (t < w.to) {
        allSlots.push(new Date(t));
        t = new Date(t.getTime() + SLOT_DURATION_MIN * 60 * 1000);
      }
    }

    const bookedTimes = new Set();
    for (const evt of (eventsData.events || [])) {
      if (![2, 4].includes(evt.eventType)) continue;
      if (evt.resourceId !== targetResource) continue;
      bookedTimes.add(new Date(evt.duration.start).getTime());
    }

    const available = allSlots
      .filter(s => !bookedTimes.has(s.getTime()))
      .map(s => {
        const end = new Date(s.getTime() + SLOT_DURATION_MIN * 60 * 1000);
        return {
          start: s.toISOString(),
          end: end.toISOString(),
          startLocal: s.toLocaleTimeString('fi-FI', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Helsinki' }),
        };
      });

    return {
      siteId,
      siteName: mapping.name,
      calendarId: calId,
      resourceId: targetResource,
      date,
      totalSlots: allSlots.length,
      bookedSlots: bookedTimes.size,
      availableSlots: available.length,
      slots: available,
    };
  }

  /**
   * Create a booking (sale + calendar event + task) in one step.
   *
   * This is the main entry point for WhatsApp agent bookings.
   */
  async createBooking(siteId, {
    registrationNumber,
    vehicleClass = 'M1',
    vehicleGroup = '155',
    engineType = '01',
    customerFirstName,
    customerLastName,
    customerPhone,
    customerEmail = '',
    customerId = null,
    slotStart,
    slotEnd,
    productId = DEFAULT_PRODUCT_ID,
    workTypeId = DEFAULT_WORK_TYPE_ID,
  }) {
    const mapping = this._siteCalendar(siteId);
    const calId = mapping.calendarId;
    const resId = mapping.onlineResource;

    if (!slotEnd) {
      const s = new Date(slotStart);
      slotEnd = new Date(s.getTime() + SLOT_DURATION_MIN * 60 * 1000).toISOString();
    }

    const customer = customerId
      ? { id: customerId }
      : {
          companyCustomer: false,
          firstName: customerFirstName,
          lastName: customerLastName,
          phone: customerPhone,
          email: customerEmail,
          inspection_reminder: ['sms'],
          marketing_permission: [],
          settings: { language: 'fi' },
        };

    const payload = {
      chainId: parseInt(CHAIN_ID),
      stationId: siteId,
      source: 2,
      customer,
      tasks: [{
        vehicle: {
          registrationNumber,
          vehicleClass: { id: vehicleClass },
          vehicleGroup: { id: vehicleGroup },
          engineType: { id: engineType },
        },
        event: {
          calendarId: calId,
          resourceId: resId,
          eventType: 4,
          cost: 1,
          duration: {
            allDay: false,
            start: slotStart,
            end: slotEnd,
          },
        },
        basket: {
          basketType: 1,
          items: [{
            productId,
            name: 'Määräaikaiskatsastus + lakisääteinen mittaus',
            quantity: { value: 1, unit: 'pcs' },
            workTypeId,
            workDuration: SLOT_DURATION_MIN,
            price: { value: 61.25, includesVat: true },
            unitPrice: { value: 61.25, includesVat: true },
            vatCode: { id: 5 },
          }],
        },
      }],
    };

    const result = await this._post(
      `/chains/${CHAIN_ID}/stations/${siteId}/sales`,
      payload
    );

    const sale = result.sale || {};
    const task = (sale.tasks || [])[0] || {};
    return {
      saleId: sale.id,
      taskId: task.id,
      eventId: task.event?.id,
      registrationNumber,
      time: { start: slotStart, end: slotEnd },
      site: { id: siteId, name: mapping.name },
      price: sale.totalPrice,
      serviceCardUrl: sale.serviceCardUrl,
      raw: result,
    };
  }

  async updateCalendarEvent(eventId, eventData) {
    return this._put(`/calendar/event/${eventId}`, eventData);
  }

  async getEventLog(eventId) {
    return this._get(`/calendar/event/${eventId}/log`);
  }

  async createCapacityEvent(calendarId, data) {
    return this._post(`/calendar/${calendarId}/capacity-event`, data);
  }

  async updateCapacityEvent(eventId, data) {
    return this._put(`/calendar/capacity-event/${eventId}`, data);
  }

  async deleteCapacityEvent(eventId) {
    return this._delete(`/calendar/capacity-event/${eventId}`);
  }

  // ── Reception / Sales ──

  async createSale(stationId, data) {
    return this._post(`/chains/${CHAIN_ID}/stations/${stationId}/sales`, data);
  }

  async getSale(stationId, saleId) {
    return this._get(`/chains/${CHAIN_ID}/stations/${stationId}/sales/${saleId}`);
  }

  async updateSale(stationId, saleId, data) {
    return this._put(`/chains/${CHAIN_ID}/stations/${stationId}/sales/${saleId}`, data);
  }

  async deleteSale(stationId, saleId) {
    return this._delete(`/chains/${CHAIN_ID}/stations/${stationId}/sales/${saleId}`);
  }

  async searchSales(stationId, params = {}) {
    return this._get(`/chains/${CHAIN_ID}/stations/${stationId}/sales/search/`, params);
  }

  async getSaleFromCalendarEvent(stationId, eventId) {
    return this._get(`/chains/${CHAIN_ID}/stations/${stationId}/sales/from-calendar/${eventId}`);
  }

  async updateSaleTaskStatus(stationId, saleId, data) {
    return this._put(`/chains/${CHAIN_ID}/stations/${stationId}/sales/${saleId}/tasks/status`, data);
  }

  async atjSearch(stationId, data) {
    return this._post(`/reception/${CHAIN_ID}/${stationId}/atjSearch`, data);
  }

  async getReceptionOptions() {
    return this._get('/reception/options');
  }

  async getVehicleOptions() {
    return this._get(`/reception/${CHAIN_ID}/options`);
  }

  async receptionCustomerSearch(stationId, query) {
    return this._get(`/reception/${CHAIN_ID}/${stationId}/customers`, { search: query });
  }

  // ── Campaigns ──

  async getCampaigns() {
    return this._get(`/chains/${CHAIN_ID}/campaigns`);
  }

  async createCampaign(data) {
    return this._post(`/chains/${CHAIN_ID}/campaigns`, data);
  }

  async getCampaign(campaignId) {
    return this._get(`/chains/${CHAIN_ID}/campaigns/${campaignId}`);
  }

  async updateCampaign(campaignId, data) {
    return this._put(`/chains/${CHAIN_ID}/campaigns/${campaignId}`, data);
  }

  async updateCampaignProducts(campaignId, products) {
    return this._put(`/chains/${CHAIN_ID}/campaigns/${campaignId}/products`, products);
  }

  async updateCampaignSites(campaignId, sites) {
    return this._put(`/chains/${CHAIN_ID}/campaigns/${campaignId}/sites`, sites);
  }

  async getCampaignsBySite(stationId) {
    return this._get(`/chains/${CHAIN_ID}/stations/${stationId}/campaigns`);
  }

  // ── Tasks (Inspection) ──

  async getStationTasks(stationId) {
    return this._get(`/chains/${CHAIN_ID}/stations/${stationId}/tasks`);
  }

  async getTaskCertificate(stationId, taskId) {
    return this._get(`/chains/${CHAIN_ID}/stations/${stationId}/tasks/${taskId}/certificate`);
  }

  // ── Products ──

  async getProducts(stationId, { limit = 100 } = {}) {
    return this._get(`/chains/${CHAIN_ID}/stations/${stationId}/products`, { limit });
  }

  async getChainProducts() {
    return this._get(`/chains/${CHAIN_ID}/products`);
  }

  // ── Static config ──

  static get siteCalendarMap() { return SITE_CALENDAR_MAP; }
  static get defaultProductId() { return DEFAULT_PRODUCT_ID; }
  static get defaultWorkTypeId() { return DEFAULT_WORK_TYPE_ID; }
}

module.exports = { KapaClient };
