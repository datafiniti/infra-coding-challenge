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
    // split into 20 chunks of 500 records each (1 record is 2 items- action and record)
    for (let i = 0; i < 100; i++) {
        chunks[i] = [];
    }
    do {
        record = await redis.rpop('records');
        record && records.push(record);
        // record && await elasticsearch.index({ index: 'records', type: 'all', body: record });
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
    // records.forEach(rec => {
    //     // indicate we're indexing an item
    //     bulkBody.push({
    //         index: {
    //             _index: 'records',
    //             _type: 'all'
    //         }
    //     });
    //     bulkBody.push(rec); // record to be indexed
    // });
    // console.log(bulkBody.length);
    for (let i = 0; i < chunks.length; i++) {
        await elasticsearch.bulk({ body: chunks[i] });
    }
    // await elasticsearch.bulk({ body: chunks[0] });
    // await elasticsearch.bulk({ body: chunks[1] });
    // await elasticsearch.bulk({ body: chunks[2] });
    // await elasticsearch.bulk({ body: chunks[3] });
    // await Promise.all(chunks.map(chunk => {
    //     return new Promise((resolve, reject) => {
    //         elasticsearch.bulk({ body: chunk })
    //             .then(resp => resolve(resp))
    //             .catch(err => reject(err));
    //     })
    // }))
    //     .catch(err => console.log(err));

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