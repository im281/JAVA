package componentDetection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ComponentDetectionMRDriver {
	
	public static void main(String[] args) throws Exception {
		
		//1) detect Isotope clusters
		RunClusterDetection(args);

		//2) detect features
		RunFeatureDetection(args);
		
		//3)Align all features independently
		RunAlignment(args);

		//4) group features -> Final list of components
		//RunComponentAssembly(args);

		//5) Assign elemental composition (Identity)
		//RunElementalComposition(args);

	}

	private static void RunClusterDetection(String[] args) throws Exception {

		Configuration conf = new Configuration();
       
		Job job = new Job(conf, "Cluster Detection");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(IsotopeClusterMapper.class);
		job.setReducerClass(IsotopeClusterReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
	
	private static void RunFeatureDetection(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Feature Detection");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(FeatureDetectMapper.class);
		job.setReducerClass(FeatureDetectReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
	}
	
	private static void RunAlignment(String[] args) throws Exception {
		
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Chromatographic Alighment");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(AlignmentMapper.class);
		job.setReducerClass(AlignmentReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[3]));
		FileOutputFormat.setOutputPath(job, new Path(args[4]));

		job.waitForCompletion(true);
	}
	
	private static void RunComponentAssembly(String[] args) throws Exception {
		
		//TODO
	}
	
	private static void RunElementalComposition(String[] args) throws Exception {
		
		//TODO
	}
	

}
