const Redis = require('ioredis');
const elasticsearch = require('./clients/elasticsearch');

const { createRecord, setupElasticsearch } = require('./utils');

(async () => {
    await setupElasticsearch();
    await seedRedis(10000);
    await importRecords();
})();

async function importRecords() {
    console.log('importing records from redis -> elasticsearch');

    const redis = new Redis({ host: 'redis' });
    let record;
    let records = [];
    let chunks = [];

    for (let i = 0; i < 100; i++) {
        chunks[i] = [];
    }
    do {
        record = await redis.rpop('records');
        record && records.push(record);

    } while (record);
    console.log('splitting...');

    let k = 0;
    //assemble bulk body
    for (let i = 0; i < 100; i++) {
        for (let j = 0; j < Math.floor(records.length / 100); j++) {
            chunks[i].push({
                index: {
                    _index: 'records',
                    _type: 'all'
                }
            });
            chunks[i].push(records[k]);
            k++
        }
    }

    for (let i = 0; i < chunks.length; i++) {
        await elasticsearch.bulk({ body: chunks[i] });
    }

    await redis.disconnect();
}

async function seedRedis(numRecords) {
    const redis = new Redis({ host: 'redis' });

    console.log('flushing redis');
    await redis.flushall();

    for (let i = 0; i < numRecords; i += 1) {

        await redis.rpush('records', JSON.stringify(createRecord()));

        if (i % (numRecords / 10) === 0) {
            console.log(`${i} records seeded in redis`);
        }
    }

    console.log('done seeding redis');
    await redis.disconnect();
}