package com.redislabs.benchmark;

import com.redislabs.benchmark.client.RedisClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by Guy on 9/21/2014.
 */
public class RedisPubSubSimpleExample {

    static final Logger logger = LogManager.getLogger(RedisPubSubSimpleExample.class.getName());

    public static void main(String[] args) {


        final RedisClient client  = new RedisClient( "54.167.224.136", 13093,10,"");
        final String channel = "JuStOne";

        //start subscribing
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("Starting listening ");
                    client.subscribe(channel);
                    System.out.println("End listening ");
                } catch (Exception e) {
                    logger.error("Subscribing failed.", e);
                }
            }
        }).start();


        //start sending messages
        //start subscribing
        System.out.println("Starting sending messages  ");

        for (int i = 0; i < 100; i++) {

            client.sendMessage( channel, "This is message " + i );

        }



        System.exit(1);

    }

}
