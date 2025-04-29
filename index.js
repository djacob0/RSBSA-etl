const cron = require('node-cron');
const { sourcePool, targetPool } = require('./config/db');
const EtlService = require('./services/etlService');

const etlService = new EtlService(sourcePool, targetPool);

cron.schedule('* * * * *', async () => {
  try {
    const result = await etlService.runEtlProcess();

    if (result.processed === 0 && result.skipped === 0) {
      console.log('No valid data to process - skipping');
    }
  } catch (error) {
    console.error('ETL job failed:', error);
  }
});

if (process.argv.includes('--run-now')) {
  (async () => {
    try {
      await etlService.runEtlProcess();
    } finally {
      await sourcePool.end();
      await targetPool.end();
    }
  })();
} else {
  console.log('RSBSA ETL scheduler to Aggregation Hub started, will run per minute.');
}