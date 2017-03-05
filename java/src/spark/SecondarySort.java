import org.apache.spark.api.java.JavaRDD;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.List;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class SecondarySort {
	
	public static void main(String args[]) throws Exception{
		//Step 1 : Read input parameters and validate them
		if(args.length < 2){
			System.err.println("Input/Output parameter is missing");
			System.exit(1);
		}
		//Step 2: Connect to Spark master
		final JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Secondary Sort").setMaster("local"));
		
		//Step 3: Create JavaRDD
		JavaRDD<String> lines = sc.textFile(args[0], 1); 
		
		//Step 4: Create key value pairs from JavaRDD
		JavaPairRDD<String,Double> pairs = 
				lines.mapToPair(new PairFunction<String,String,Double>(){
					public Tuple2<String,Double> call(String s){
						String[] tokens = s.split(",");
						String yearMonth = tokens[0].trim()+"-"+tokens[1].trim();
						Double temperature = new Double(tokens[3].trim());
						return new Tuple2<String, Double>(yearMonth, temperature);
					}
				});
				
		
		//Step 5: Group and sort JavaPairRDD Elements by the key {year-month}
		JavaPairRDD<String,Iterable<Double>> groups = pairs.groupByKey().sortByKey();
		
		//Step 6: Sort Reducer's value in memory
		JavaPairRDD<String,Iterable<Double>> sorted = 
				groups.mapValues(new Function<Iterable<Double>, //input
											  Iterable<Double>  //output
							>(){
					public Iterable<Double> call(Iterable<Double> s){
						
						List<Double> newList = new ArrayList<Double>();
						CollectionUtils.addAll(newList, s.iterator());
						Collections.sort(newList);
						
						return newList;
					}
				});

		sorted.saveAsTextFile(args[1]); 
		
	}
	
	
	public static class TupleComparator implements Comparator<Tuple2<Integer, Double>>, Serializable {

        @Override
        public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
            return -1*Double.compare(o2._2(), o1._2());
        }
	}
	
}
