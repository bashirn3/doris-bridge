require('dotenv').config();
const axios = require('axios');

const API_BASE = process.env.DORIS_API_BASE;
const API_CODE = process.env.DORIS_API_CODE;
const CHAIN_ID = process.env.CHAIN_ID;

class DorisClient {
  constructor(auth) {
    this.auth = auth;
    this.http = axios.create({
      baseURL: API_BASE,
      timeout: 30000,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  async _headers() {
    const token = await this.auth.getSpaToken();
    return { 'X-DORIS-AUTH': `Bearer ${token}` };
  }

  async _crud(procName, data = {}) {
    const headers = await this._headers();
    const res = await this.http.post(
      `/chain/${CHAIN_ID}/DorisCRUD?code=${API_CODE}`,
      { procName, data: { chainId: parseInt(CHAIN_ID), ...data } },
      { headers }
    );
    return res.data;
  }

  async _get(path) {
    const headers = await this._headers();
    const sep = path.includes('?') ? '&' : '?';
    const res = await this.http.get(`${path}${sep}code=${API_CODE}`, { headers });
    return res.data;
  }

  // ── User / Profile ──

  async getProfile() {
    return this._get('/user/profile');
  }

  async getChains() {
    return this._get('/chains');
  }

  // ── Sites ──

  async getSites() {
    return this._crud('GetSitesInChain');
  }

  async getSite(siteId) {
    return this._crud('GetSite', { siteId });
  }

  async getSiteGroups() {
    return this._crud('GetSiteGroupsInChain');
  }

  async getCompanies() {
    return this._crud('GetCompaniesInChain');
  }

  // ── Job Queue ──

  async getJobQueue(siteId) {
    return this._crud('GetJobQueue', { siteId });
  }

  async getJobQueueJob(siteId, jobId) {
    return this._crud('GetJobQueueJob', { siteId, jobId });
  }

  async getJobsInSite(siteId) {
    return this._crud('GetJobsInSite', { siteId });
  }

  async getJobHistory(siteId) {
    return this._crud('GetJobHistory', { siteId });
  }

  async getJobStatistics(siteId) {
    return this._crud('GetJobStatistics', { siteId });
  }

  async getJob(jobId) {
    return this._get(`/chain/${CHAIN_ID}/job/${jobId}`);
  }

  async annulJob(data) {
    return this._crud('annulJob', data);
  }

  // ── Job Types ──

  async getJobTypes() {
    return this._crud('GetJobTypesInChain');
  }

  async getJobType(id) {
    return this._crud('GetJobType', { id });
  }

  async getBaseJobTypes() {
    return this._crud('GetBaseJobTypes');
  }

  // ── Vehicle Data ──

  async getVehicleGroups() {
    return this._crud('GetVehicleGroupsInChain');
  }

  async getVehicleGroup(id) {
    return this._crud('GetVehicleGroup', { id });
  }

  async getVehicleClasses() {
    return this._crud('GetVehicleClassesInChain');
  }

  // ── Inspection Lines ──

  async getInspectionLines(siteId) {
    return this._crud('GetInspectionLinesInSite', { siteId });
  }

  async getDefaultInspectionLines(siteId) {
    return this._crud('GetDefaultInspectionLinesInSite', { siteId });
  }

  // ── Users ──

  async getUsersInSite(siteId) {
    return this._crud('GetUsersInSite', { siteId });
  }

  async getInspectionUsers() {
    return this._crud('GetInspectionUsersInChain');
  }

  async getUserList() {
    return this._get(`/chain/${CHAIN_ID}/userList`);
  }
}

module.exports = { DorisClient };
