require('dotenv').config();
const { B2CAuth } = require('./b2c-auth');
const { DorisClient } = require('./doris-client');
const { KapaClient } = require('./kapa-client');

async function main() {
  const auth = new B2CAuth();
  const doris = new DorisClient(auth);
  const kapa = new KapaClient(auth);

  console.log('=== Testing SPA (Azure Functions) Auth ===\n');

  try {
    const sites = await doris.getSites();
    console.log('Sites:', JSON.stringify(sites, null, 2).slice(0, 500));
    console.log('\n✓ SPA auth works\n');
  } catch (e) {
    console.error('✗ SPA auth failed:', e.message);
  }

  console.log('=== Testing KAPA Auth ===\n');

  try {
    const customers = await kapa.searchCustomers('', { limit: 3 });
    console.log('Customers:', JSON.stringify(customers, null, 2).slice(0, 500));
    console.log('\n✓ KAPA auth works\n');
  } catch (e) {
    console.error('✗ KAPA auth failed:', e.message);
  }

  console.log('=== Testing Calendar ===\n');

  try {
    const resources = await kapa.getCalendarResources(58);
    console.log('Calendar resources (site 58):', JSON.stringify(resources, null, 2).slice(0, 500));
    console.log('\n✓ Calendar access works\n');
  } catch (e) {
    console.error('✗ Calendar access failed:', e.message);
  }

  console.log('=== Done ===');
}

main().catch(e => {
  console.error('Fatal:', e);
  process.exit(1);
});
