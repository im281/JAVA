package componentDetection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Function2;
import scala.Tuple2;

public class Test2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().appName("Component Detection Test 2").getOrCreate().newSession();
		// end boiler plate

		//JavaRDD<String> scans = spark.read().textFile(args[0]).javaRDD();
		
		// read all text files (raw files converted to text)
		JavaRDD<String> scans = spark.read().textFile("Data/CD/blacktea").javaRDD();

		/*
		 * lines.flatMapToPair(line -> { Detector detector = new Detector();
		 * return detector.GetClusters(line).stream().map(cluster -> { String
		 * cKey = detector.WriteClusterKey(cluster); String cValue =
		 * detector.WriteClusterValue(cluster); return new Tuple2<>(cKey,
		 * cValue); }).iterator(); });
		 */

		PairFlatMapFunction<String, String, String> keyData = new PairFlatMapFunction<String, String, String>() {
			@Override
			public Iterator<Tuple2<String, String>> call(String scan) throws Exception {
				System.out.println("Inside Isotope Cluster Flat Map!");
				Detector detector = new Detector();
				return detector.GetClusters(scan).stream().map(cluster -> {
					String cKey = detector.WriteClusterKey(cluster);
					String cValue = detector.WriteClusterValue(cluster);
					return new Tuple2<>(cKey, cValue);
				}).iterator();
			}
		};

		JavaPairRDD<String, String> clusters = scans.flatMapToPair(keyData);

		List<Tuple2<String, String>> output = clusters.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		JavaPairRDD<String, Iterable<String>> groupedClusters = clusters.groupByKey();

		System.out.print(groupedClusters.collect().toString());

		JavaPairRDD<String, String> features = clusters.groupByKey().mapValues(values -> {

			System.out.println("Inside Feature Detection Reduce !");
			Detector c = new Detector();
			ArrayList<IsotopeCluster> clusters1 = (ArrayList<IsotopeCluster>) StreamSupport
					.stream(values.spliterator(), false).map(c::GetClustersfromKey).collect(Collectors.toList());
			return c.WriteFeatureValue(c.DetectFeature(clusters1));
		});

		System.out.print(features.collect().toString());

		List<Tuple2<String, String>> output1 = features.collect();
		for (Tuple2<?, ?> tuple : output1) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

	}
}
