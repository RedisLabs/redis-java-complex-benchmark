package com.redislabs.benchmark;

import com.google.common.base.Stopwatch;
import com.redislabs.benchmark.client.RedisClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Guy on 8/18/2014.
 */
public class BenchmarkAccount implements Runnable {


    private final RedisClient redisClient;
    private final Stopwatch  stopwatch = Stopwatch.createUnstarted();
    private final int topBusiness;
    private final Logger logger = LogManager.getLogger(BenchmarkAccount.class.getName());
    private final int accountsPrefix;


    public AtomicInteger getOperationCounter() {
        return operationCounter;
    }

    private final AtomicInteger operationCounter = new AtomicInteger();
    private final AtomicInteger errorCounter = new AtomicInteger();

    public List<Long> getLatencyList() {
        return latencyList;
    }

    public int getErrorCount()
    {
        return  errorCounter.get();
    }

    private final List<Long> latencyList = new ArrayList<>();

    public Boolean getComplete() {
        return complete;
    }

    private Boolean complete = false;



    private boolean continueRun;

    /**
     * set up there needed parameters
     * @param redisClient redis client
     * @param topBusiness how many business need to be tested
     */
    public BenchmarkAccount(RedisClient redisClient, int topBusiness, int accountsPrefix)
    {
        this.redisClient = redisClient;
        this.topBusiness = topBusiness;
        this.accountsPrefix = accountsPrefix;
        continueRun=true;
    }

    /**
     * get the correct account names and execute the union store method
     * @param businessName
     * @return
     */
    public Long unionBusiness(String businessName)
    {

        String account1 = BenchmarkUtils.getAccountName(businessName,0);
        String account2 = BenchmarkUtils.getAccountName(businessName,1);
        String account3 = BenchmarkUtils.getAccountName(businessName,2);
        String account4 = BenchmarkUtils.getAccountName(businessName,3);

        String outputZset  = businessName + ":" + UUID.randomUUID().toString();

        return redisClient.unionZset(outputZset,account1,account2,account3,account4);
    }

    @Override
    public void run() {

        //this is made to make sure each thread work on different business account
        /***
         * Changing current logic to validate each thread will go to a different shard
         */
        //we will set the initial counter
        int i = accountsPrefix;

        logger.info("Starting on prefix " + i);
        //continue to run until the flag is set to false
        while(continueRun) {

            stopwatch.start();
            Long results =  unionBusiness(BenchmarkUtils.getBuisnessName(  i ));
            if ( results == -1 )
                errorCounter.incrementAndGet();

            //command was executed properly -- need to maek this a parameter
            if ( results != 80000 )
                logger.error("we didnt make a union on all items  results " + results + " business " + i );

            stopwatch.stop();
            
            latencyList.add(stopwatch.elapsed(TimeUnit.MILLISECONDS));
            //logger.debug ("Time it took to union - " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " results - " + results);
            stopwatch.reset();
            if ( (operationCounter.incrementAndGet() % 10000) == 0 )
            {
                logger.info("Processed 10k ");
            }
            //once we moved the top business reset it back to zero otherwise increment by one
            if (( topBusiness - 1) == i )
                i++;
            else
                i=0;
        }

        complete = true;

    }

    public void setContinueRun(boolean continueRun) {
        this.continueRun = continueRun;
    }

}
