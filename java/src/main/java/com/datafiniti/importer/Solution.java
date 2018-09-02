package com.datafiniti.importer;

import redis.clients.jedis.Jedis;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


class InsertRunnable implements Runnable {
    private String record;
    public InsertRunnable(String record) {
        this.record = record;
    }

    public void run() {
        try {
            ESClient.insert(record);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class Solution {
    public static void run() throws IOException {
        ESClient.connect();
        ESClient.setup();

        seedRedis(20_000);

        importRecords();
    }

    public static void importRecords() throws IOException {
        Jedis jedis = new Jedis("redis");

        System.out.println("importing records from redis -> elasticsearch");

        ExecutorService pool = Executors.newFixedThreadPool(250);

        while (jedis.llen("records") != 0) {
            pool.execute(new InsertRunnable(jedis.lpop("records")));
        }

        System.out.println("done importing.");

        pool.shutdown();

        while (true) {
            if (pool.isTerminated()) {
                System.out.println("ThreadPool finished Indexing records");
                break;
            }
        }

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