package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class RddAndPairRdd extends SparkContext {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        System.out.println("what is the difference in rdd and pair rdd in spark-java api");
        // spark context 
        JavaSparkContext sc=getSparkContext();
   
        List<Integer> arr=new ArrayList<>();
        arr.add(1);
        arr.add(12);
        arr.add(13);
        JavaRDD<Integer> myRdd=sc.parallelize(arr);


        sc.close();

    }
}
