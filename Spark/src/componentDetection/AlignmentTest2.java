package componentDetection;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class AlignmentTest2 {

	public static void main(String[] args) {

		// boiler plate needed to run locally
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().appName("Test").getOrCreate().newSession();
		// end boiler plate
		
		//instantiate detector class
		Detector d = new Detector();

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String x) {
				return new Tuple2(x.split("['\t'_']")[0], x.split("['\t'_']")[2]);
			}
		};

		JavaPairRDD<String, String> pairs = lines.mapToPair(keyData);

		List<Tuple2<String, String>> output = pairs.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		JavaPairRDD<String, Iterable<String>> groupedFeatures = pairs.groupByKey();

		List<Tuple2<String, Iterable<String>>> features = groupedFeatures.collect();
		for (Tuple2<?, ?> tuple1 : features) {
			System.out.println(tuple1._1() + ": " + tuple1._2());
		}

		List<Tuple2<String, String>> aligmnentOffsets = pairs.groupByKey().mapValues(values -> {		
			return d.GetFeatureAlignmentOffset(values);
			}).collect();
		
		for (Tuple2<?, ?> tuple1 : aligmnentOffsets) {
			System.out.println(tuple1._1() + ": " + tuple1._2());
		}		
	}
}
