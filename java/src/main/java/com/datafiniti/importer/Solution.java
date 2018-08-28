package com.datafiniti.importer;

import redis.clients.jedis.Jedis;
import java.io.IOException;

class Solution {
    public static void run() throws IOException {
        ESClient.connect();
        ESClient.setup();

        seedRedis(10_000);

        int threadCount = 0;
        int threadCapacity = 7;

        do {
            Thread worker = createWorkerThread();
            worker.start();
            System.out.println("=> Just spun up a new worker thread");
            threadCount++;
        } while (threadCount != threadCapacity);

    }

    /**
     * Creates a new thread to begin processessing Elastisearch inserts concurrently
     */
    public static Thread createWorkerThread() {
        return new Thread(() -> {
            try {
				importRecords();
			} catch (IOException e) {
				e.printStackTrace();
			}
        });
    }


    public static void importRecords() throws IOException {
        Jedis jedis = new Jedis("redis");

        System.out.println("importing records from redis -> elasticsearch");

        String record;
        while ((record = jedis.rpop("records")) != null) {
            ESClient.insert(record);
        }

        System.out.println("done importing.");
    }

    public static void seedRedis(Integer numRecords) throws IOException {
        Jedis jedis = new Jedis("redis");

        System.out.println("flushing redis");
        jedis.flushAll();

        System.out.println("seeding redis with " + numRecords + " records.");
        for (int i = 0; i < numRecords; i += 1) {
            jedis.rpush("records", Utils.createRecord());

            if (i % (numRecords / 10) == 0) {
                System.out.println(i + " records seeded.");
            }
        }

        jedis.close();
    }
}