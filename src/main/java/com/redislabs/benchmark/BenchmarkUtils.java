package com.redislabs.benchmark;

/**
 * Created by Guy on 8/18/2014.
 */
public  class BenchmarkUtils {


    public static String getBuisnessName(int iterator) {

            return "Business" + iterator;

    }

    public static String getAccountName(String businessName,int iterator) {

        return businessName + ":Account" + iterator;

    }


}
