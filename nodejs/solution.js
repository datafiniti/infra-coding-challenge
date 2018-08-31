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
    let records;
    let i = 99
    do {
        records = await redis.lrange('records',0, i);
        redis.ltrim('records', 0, i)
        for(let record in records){
            await elasticsearch.bulk({index:{ index: 'records', type: 'all'} + '\n'}, {body: record} + '\n');    
        }
        i+=99;
    } while (record);

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