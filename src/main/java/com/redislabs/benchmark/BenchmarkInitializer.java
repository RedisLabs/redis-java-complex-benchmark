package com.redislabs.benchmark;

import com.google.common.base.Stopwatch;
import com.redislabs.benchmark.client.RedisClient;
import com.redislabs.benchmark.common.BusinessTransaction;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Guy on 8/11/2014.
 */
public class BenchmarkInitializer {

    private final RedisClient client;
    private Random rnd = new Random();

    public BenchmarkInitializer(RedisClient client)
    {
        this.client = client;
    }


    /**
     *  this method will initialized all data
     */
    public void initializeSet(int numberOfSets)
    {
        Stopwatch sw = Stopwatch.createUnstarted();

        sw.start();
        initializeBuisnessAccounts(numberOfSets);
        sw.stop();
        System.out.println("Full data init " + sw.elapsed(TimeUnit.MINUTES));
    }

    private void initializeBuisnessAccounts(int numberOfBusiness) {

        Stopwatch sw = Stopwatch.createUnstarted();

        for (int i = 0; i < numberOfBusiness; i++) {

            String businessName = BenchmarkUtils.getBuisnessName(i);

            sw.start();
            createSimulateTransactionForBusiness(businessName, 4, 20000);
            sw.stop();
            System.out.println(i +  "Business was created in " + sw.elapsed(TimeUnit.SECONDS));
            sw.reset();

        }


    }

    private void createSimulateTransactionForBusiness(String businessName, int numberOfAccounts, int numberOfTransactions) {

        Dictionary<String,List<BusinessTransaction>> buisnessData = new Hashtable<String, List<BusinessTransaction>>(numberOfAccounts);

        for (int i = 0; i < numberOfAccounts; i++) {

            List<BusinessTransaction> fullTransactions  = new ArrayList<BusinessTransaction>(numberOfAccounts);
            String accountTransactionNameKey = BenchmarkUtils.getAccountName(businessName,i);

            for (long j = 0; j < numberOfTransactions; j++) {

                BusinessTransaction bt  = new BusinessTransaction();
                bt.setAmount(rnd.nextDouble());
                bt.setTimestamp(j);
                fullTransactions.add(bt);
            }

            buisnessData.put(accountTransactionNameKey,fullTransactions);
            // client.setZSet(accountTransactionNameKey,fullTransactions);

        }

        client.setZSet(buisnessData);

    }


}
