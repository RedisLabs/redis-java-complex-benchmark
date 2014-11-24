package com.redislabs.benchmark;

import com.google.common.base.Stopwatch;
import com.redislabs.benchmark.client.RedisClient;
import org.apache.commons.collections.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Guy on 8/11/2014.
 */
public class BenchmarkRunner {

    static final Logger logger = LogManager.getLogger(BenchmarkRunner.class.getName());
    private String host;
    private int port;
    private int timeToRunInSeconds;
    private boolean init;
    private int numberOfAccounts=500;
    private int numberOfThreads;
    List<Long> totalLatencies = new ArrayList<>();
    List<Integer> errorCount = new ArrayList<>();

    //this arrays will include business that belong to the same shard
//    private int[] shard1 = {6, 45, 127,58,32,104,96};
//    private int[] shard2 = {180,244,197,9,248,153};
//    private int[] shard3 = {41,131,118,191181,18};
//    private int[] shard4 = {154,257,157,53,288,266};
//    private int[] shard5 = {56,274,85,98,218,249,75};
    private String password;


    public static void main(String[] args) throws InterruptedException {

        ///verify that you have any parameters before starting the app
        if (args.length == 0) {
            logger.error(" benchmark was started without parameters exiting ");
            return;
        }


        BenchmarkRunner benchmarkRunner = new BenchmarkRunner();

        benchmarkRunner.run(args);

//


        //if initialize do this

        //if running the script do this


    }

    /**
     * Run the benchmark
     * @param args args
     * @throws InterruptedException
     */
    private void run(String[] args) throws InterruptedException {


        //check to see if all the parameters are there and set them up
        if(!setBenchmarkParameters(args))
        {
            return;
        }
        //create redis client wrapper with correct configuration
        RedisClient redisClient = new RedisClient(host, port, 2000,password);

        //only perform load data without running the benchmark
        //if we are loading data stop the test
        if ( init )
        {
            BenchmarkInitializer benchmarkInitialize = new BenchmarkInitializer(redisClient);
            benchmarkInitialize.initializeSet(numberOfAccounts);
            logger.info(" Init of data completed ");
            return;
        }
        //check the full test time and stop the test once the time is over
        Stopwatch runningFullTime = Stopwatch.createStarted();
        //workers list
        List<Runnable> runnerWorkers = new ArrayList<>();
        //executer that will create the amount of thread pool -- can be increased to support more then 5 threads
        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < numberOfThreads; i++) {
            Runnable worker = new BenchmarkAccount(redisClient, numberOfAccounts,i);
            executor.execute(worker);
            runnerWorkers.add(worker);
        }


        //wait until the requested time of run minus one second to prepare for the worker stop
        executor.awaitTermination(timeToRunInSeconds - 1, TimeUnit.SECONDS);
        //start shutdown process
        executor.shutdown();
        while (!executor.isTerminated()) {
            if (runningFullTime.elapsed(TimeUnit.SECONDS) > timeToRunInSeconds) {
                for (Runnable worker1 : runnerWorkers )
                {
                    //set stop flag on worker so the worker will exit the loop
                    ((BenchmarkAccount )worker1).setContinueRun(false);
                    Thread.sleep(10);
                }
            }
            else
                Thread.sleep(10);

        }

        //get results from all completed tests.
        for (Runnable worker1 : runnerWorkers )
        {
            //stop benchmark
            if (((BenchmarkAccount )worker1).getComplete())
            {

                //print all latency just as a test -- this will validate the avg works
//                for ( Long latency : ((BenchmarkAccount )worker1) .getLatencyList())
//                {
//                    logger.info("This is the latency of a single run " + latency);
//                }
                //join all latency results into one set
                totalLatencies =  ListUtils.union(totalLatencies,((BenchmarkAccount )worker1).getLatencyList());
                //add the counter of error to see if it works
                errorCount.add(((BenchmarkAccount )worker1).getErrorCount());
            }
            else
                logger.error("************* Error benchmark is not done  ");
        }

        //calculate the average latency
        Double totalLatencyamount = (double) 0;
        for (Long latency : totalLatencies )
            totalLatencyamount += latency;

        long totalErrors=0;
        for (Integer errors : errorCount)
            totalErrors += errors;

        Double averageLatency = totalLatencyamount / totalLatencies.size();

        Double tps =  (double)totalLatencies.size() / timeToRunInSeconds;

        //print the results, they can be printed also to the log
        System.out.println("Finished all threads tps " + tps + " average " + averageLatency + " number of errors " + totalErrors);



    }

    public boolean setBenchmarkParameters(String[] benchmarkParameters) {

        logger.debug("Setting Server");

        if ( benchmarkParameters.length > 0  )
                this.init = (benchmarkParameters[0].equalsIgnoreCase("init"));


        if ( benchmarkParameters.length > 1  )
            this.host = benchmarkParameters[1];
        else{
            logger.error("No host in input parameters ");
            return false;
        }

        if ( benchmarkParameters.length > 2  )
            this.port = Integer.parseInt(benchmarkParameters[2]);
        else {
            logger.error("No port in input parameters ");
            return false;
        }

        if ( benchmarkParameters.length > 3  )
            this.timeToRunInSeconds = Integer.parseInt(benchmarkParameters[3]);
        else {
            logger.error("No running time in input parameters ");
            return false;
        }

        if ( benchmarkParameters.length > 4  )
            this.numberOfThreads = Integer.parseInt(benchmarkParameters[4]);
        else {
            logger.error("No threads in input parameters ");
            return false;
        }

        if ( benchmarkParameters.length > 5  )
            this.numberOfAccounts = Integer.parseInt(benchmarkParameters[5]);
        else {
            logger.error("number of business in input parameters ");
            return false;
        }
        //these parameter is optional doesn't have to be supplied
        if ( benchmarkParameters.length > 6  )
            this.password = benchmarkParameters[6];

        return true;
    }

}
