package com.datafiniti.importer;

import redis.clients.jedis.Jedis;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


class InsertRunnable implements Runnable {
    String record;
    InsertRunnable(String record) {
        this.record = record;
    }

    public void run() {
        try {
            ESClient.insert(record);
        }
        catch (IOException e) {
            System.out.println("IOException: " + e);
        }
    }
}

class Solution {
    public static void run() throws IOException {
        ESClient.connect();
        ESClient.setup();

        seedRedis(10_000);

        importRecords();
    }

    public static void importRecords() throws IOException {
        Jedis jedis = new Jedis("redis");

        System.out.println("importing records from redis -> elasticsearch");

        ExecutorService pool = Executors.newFixedThreadPool(2000);

        String record;
        while ((record = jedis.rpop("records")) != null) {
            pool.execute(new InsertRunnable(record));
        }
        pool.shutdown();

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