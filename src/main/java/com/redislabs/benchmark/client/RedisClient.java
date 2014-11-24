package com.redislabs.benchmark.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.redislabs.benchmark.common.BusinessTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.*;

import java.util.Dictionary;
import java.util.Enumeration;
import java.util.List;

/**
 * Created by Guy on 8/11/2014.
 */
public class RedisClient {

    private final JedisPool pool;
    private Gson gson = new GsonBuilder().create();
    static final Logger logger = LogManager.getLogger(RedisClient.class.getName());

    public RedisClient(String host,int port,int numberOfConnection,String password)
    {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(numberOfConnection);

        //we will place the timeout as hardcoded right now
        if (password == null)
            pool = new JedisPool(config, host,port,20000);
        else
            pool = new JedisPool(config, host,port,20000,password);

    }

    public void set ( String key, Object value )
    {
        Jedis jedis = pool.getResource();
        String strValue =     gson.toJson(value);

        try {
            jedis.set(key, strValue);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public void setArray ( List<String> keys,  List<Object> values )
    {
        Jedis jedis = pool.getResource();

        String value;
        try {

            Pipeline pipeline  = jedis.pipelined();

            for (int i = 0; i < keys.size(); i++) {

                value = gson.toJson(values.get(i));
                pipeline.set(keys.get(i), value);

            }

            pipeline.sync();

        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public void setSet ( String key,  List<String> values )
    {
        Jedis jedis = pool.getResource();

        try {

            Pipeline pipeline  = jedis.pipelined();

            for (String val : values)
            {
                pipeline.sadd(key,val);
            }

            pipeline.sync();

        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public void setZSet ( String key,List<Integer> scores,  List<String> values )
    {
        Jedis jedis = pool.getResource();

        try {

            Pipeline pipeline  = jedis.pipelined();

            for (int i = 0; i < values.size(); i++) {

                pipeline.zadd(key,scores.get(i),values.get(i));


            }

            pipeline.sync();

        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }


    public void setZSet(String accountTransactionNameKey, List<BusinessTransaction> fullTransactions) {


        Jedis jedis = pool.getResource();

        try {

            Pipeline pipeline  = jedis.pipelined();

            for ( BusinessTransaction bt : fullTransactions  )
            {
                pipeline.zadd(accountTransactionNameKey,bt.getTimestamp(),bt.getAmount().toString());
            }

            pipeline.sync();

        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if (null != jedis) {
                jedis.close();
            }
        }

    }

    public Long unionZset(String outputZset, String account1, String account2, String account3, String account4)
    {
        Jedis jedis = pool.getResource();
        Long results = Long.valueOf(0);
        try {


            Pipeline pipeline = jedis.pipelined();
            redis.clients.jedis.Response<Long> response =  pipeline.zunionstore(outputZset, account1, account2, account3, account4);
            //this is comment not to loose time
            //pipeline.get(outputZset);
            pipeline.del(outputZset);

            pipeline.sync();

            results = response.get();

        }
        catch (Exception e){
            logger.error(e.getMessage());
            results = Long.valueOf(-1);
        }
        finally {
            if (null != jedis) {
                jedis.close();
            }
        }



        return results;
    }

    public void setZSet(Dictionary<String, List<BusinessTransaction>> buisnessData) {

        Jedis jedis = pool.getResource();

        try {

            Pipeline pipeline  = jedis.pipelined();


            Enumeration<String> keys = buisnessData.keys();

            while ( keys.hasMoreElements())
            {
                String accountName = keys.nextElement();

                List<BusinessTransaction> transactions =  buisnessData.get(accountName);

                for ( BusinessTransaction bt : transactions )
                {
                    pipeline.zadd(accountName,bt.getTimestamp(),bt.getAmount().toString());
                }
            }

            pipeline.sync();

        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if (null != jedis) {
                jedis.close();
            }
        }

    }

    /**
     * This method will bock as long as it is called ( need to make it none blcking to work properly )
     * @param channel
     */
    public void subscribe(String channel)
    {
        Jedis jedis = pool.getResource();

        try {

            //should be defined as class field for this example it will be local
            RedisListener listener = new RedisListener();
            jedis.subscribe(listener, channel);

        }
        catch (Exception e){
            logger.error(e.getMessage());
        }
        finally {
            if (null != jedis) {
                jedis.close();
            }
        }



    }


    public void sendMessage(String channel, String message) {

        Jedis jedis = pool.getResource();

        try {

            //should be defined as class field for this example it will be local
            jedis.publish(channel,message);

        }
        catch (Exception e){
            logger.error(e.getMessage());
        }
        finally {
            if (null != jedis) {
                jedis.close();
            }
        }


    }
}
