package com.redislabs.benchmark.client;

import redis.clients.jedis.JedisPubSub;

/**
 * Created by Guy on 9/21/2014.
 */
public class RedisListener extends JedisPubSub {


    @Override
    public void onMessage(String channel, String message) {

        System.out.println("Just got this message from " + channel + "  message itself "  + message);

    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {

        System.out.println("Just got this pm message from " + channel + "  message itself "  + message);
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {

        System.out.println("Subscribing to message " + channel + " channel " + subscribedChannels);
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {

    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {

    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {

    }
}
