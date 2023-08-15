package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class SparkContext {
    public static JavaSparkContext getSparkContext(){
        SparkConf conf=new SparkConf().setAppName("SparkCore").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        return sc;
    }
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        JavaSparkContext sc=getSparkContext();


        //will create the spark RDD(Resilient-Distribute-Dataset)
        ArrayList<Integer> arr=new ArrayList<Integer>();
        arr.add(12);
        arr.add(22);
        arr.add(32);
        arr.add(42);
        JavaRDD<Integer> myrdd=sc.parallelize(arr);

        //reduce operation
        int result = myrdd.reduce((ele1, ele2) -> ele1 + ele2);
        System.out.println("sum of myrdd elements : "+result);

        //mapping
        JavaRDD<Double> map_result = myrdd.map(e -> Math.sqrt(e));
        System.out.println("result of mapping : "+map_result);

        //printing the java rdd values
        map_result.foreach(e-> System.out.println(e));

        //no of records in the RDD
        System.out.println("no. of records : "+map_result.count());

        //map-reduce  [use for the counting the elements]
        JavaRDD<Long> map = myrdd.map(v -> 1L);
        Long finalCount=map.reduce((v1,v2)->v1+v2);
        System.out.println("Manually way of countung : "+finalCount);


        //collect function??



        //closing the spark
        sc.close();

    }
}
