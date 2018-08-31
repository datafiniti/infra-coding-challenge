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
    let processed = 0;
    let len = await redis.llen('records');
    let pageSize = 200;

    for(let i = 0; i < len; i+=pageSize){
        let records = await redis.lrange('records', i, (i + pageSize - 1));
        if (!records.length) {
            break;
        }
        processed += records.length;
        console.log(processed, 'records processed');

        let recordsToIndex = [];
        for (let record of records) {
            recordsToIndex.push({index:{_index: 'records', _type: 'all'}});
            recordsToIndex.push(record);
        }
        await elasticsearch.bulk({body: recordsToIndex});
    }

    await redis.ltrim(0, -1); // clear out redis
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