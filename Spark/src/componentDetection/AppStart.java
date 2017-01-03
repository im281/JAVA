package componentDetection;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
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

public class AppStart {

	public static void main(String[] args) {

		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().appName("Component Detection Demo").getOrCreate().newSession();
		// end boiler plate

		// read all text files (raw files converted to text) from directory
		JavaRDD<String> scans = spark.read().textFile(args[0]).javaRDD();
		
		//create isotope cluster key-value pairs
		PairFlatMapFunction<String, String, String> keyData = ClusterFlatMapPairFunction();

		//feature detect all clusters from all files
		JavaPairRDD<String, String> features = SparkFeatureDetect(scans, keyData);

		// prints out the feature key-value pairs
		List<Tuple2<String, String>> output = WriteOutFeatures(features);
		
		//add alignment offsets to features
		
		//writes features to the annotation editor text format
		//WriteResults(output,args);
		
		// groups them by keys and passes
		// each collection of isotopes to the feature detector and this time the
		// detector can return multiple features for the same isotope cluster
		/*JavaPairRDD<String, Object> featuresList = scans.flatMapToPair(keyData).groupByKey().mapValues(values -> {

			System.out.println("Inside Feature Detection Reduce !");
			Detector d = new Detector();
			ArrayList<IsotopeCluster> clusters = (ArrayList<IsotopeCluster>) StreamSupport
					.stream(values.spliterator(), false).map(d::GetClustersfromKey).collect(Collectors.toList());
			return d.WriteFeatureValues(d.DetectFeatures(clusters));
		});

		// prints out the feature key-value pairs
		List<Tuple2<String, Object>> output1 = featuresList.collect();
		System.out.println("FeatureID : FileID, MimzMass, Intensity, Apex Scan, Apex RT");
		for (Tuple2<?, ?> tuple : output1) {
			System.out.println("FeatureID " + tuple._1() + ": " + tuple._2());
		}
        */

		//WriteResults(output,args);

	}

	private static List<Tuple2<String, String>> WriteOutFeatures(JavaPairRDD<String, String> features) {
		List<Tuple2<String, String>> output = features.collect();
		System.out.println("FeatureID : FileID, MimzMass, Intensity, Apex Scan, Apex RT");
		for (Tuple2<?, ?> tuple : output) {
			System.out.println("FeatureID " + tuple._1() + ": " + tuple._2());
		}
		return output;
	}

	private static void WriteToAnnotationEditorFormat(List<Tuple2<String, String>> output) {
		// write out to Annotation Editor format
		for (Tuple2<?, ?> tuple : output) {
			String f = (String) tuple._2();
			String[] data = f.split(",");
			System.out.println(data[4] + "," + data[1] + "," + "0.3" + "," + "+MS" + "," + 1);
		}
	}

	private static JavaPairRDD<String, String> SparkFeatureDetect(JavaRDD<String> scans,
			PairFlatMapFunction<String, String, String> keyData) {
		// creates key-value pairs for Isotope cluster ID and Isotope cluster
		// groups them by keys and passes
		// each collection of isotopes to the feature detector
		JavaPairRDD<String, String> features = scans.flatMapToPair(keyData).groupByKey().mapValues(values -> {

			System.out.println("Inside Feature Detection Reduce !");
			Detector d = new Detector();
			ArrayList<IsotopeCluster> clusters = (ArrayList<IsotopeCluster>) StreamSupport
					.stream(values.spliterator(), false).map(d::GetClustersfromKey).collect(Collectors.toList());
			return d.WriteFeatureValue(d.DetectFeature(clusters));
		});
		return features;
	}

	private static PairFlatMapFunction<String, String, String> ClusterFlatMapPairFunction() {
		// Pairfunction-Reads each scan and returns a list of Isotope clusters
		// which where key-value pairs are created
		PairFlatMapFunction<String, String, String> keyData = new PairFlatMapFunction<String, String, String>() {
			@Override
			public Iterator<Tuple2<String, String>> call(String scan) throws Exception {
				System.out.println("Inside Isotope Cluster Flat Map! " + " Getting Isotopeclusters for scan# "
						+ scan.split(";")[1]);
				Detector detector = new Detector();
				return detector.GetClusters(scan).stream().map(cluster -> {
					String cKey = detector.WriteClusterKey(cluster);
					String cValue = detector.WriteClusterValue(cluster);
					return new Tuple2<>(cKey, cValue);
				}).iterator();
			}
		};
		return keyData;
	}

	private static void WriteResults(List<Tuple2<String, String>> output, String[] args) {
		BufferedWriter writer = null;	
		try {
			// create a temporary file
			String timeLog = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
			
			File logFile = new File(args[1] + File.separator +  timeLog + ".txt");
	       
			// This will output the full path where the file will be written
			// to...
			System.out.println(logFile.getCanonicalPath());

			writer = new BufferedWriter(new FileWriter(logFile));
			for (Tuple2<?, ?> tuple : output) {
				String f = (String) tuple._2();
				String[] data = f.split(",");
				writer.write(data[4] + "," + data[1] + "," + "0.3" + "," + "+MS" + "," + 1);
				writer.newLine();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// Close the writer regardless of what happens...
				writer.close();
			} catch (Exception e) {
			}
		}
	}
}
