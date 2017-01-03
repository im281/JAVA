package componentDetection;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class IsotopeClusterMapper extends Mapper<LongWritable, 
Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("Inside Isotope Cluster Map !");
		String line = value.toString();

		// Get Isotope clusters here are write out to text
		Detector detector = new Detector();
		
		//TODO:The real isotope cluster algorithm goes here
		ArrayList<IsotopeCluster> clusters = detector.GetClusters(line);

		for (int i = 0; i < clusters.size(); i++) {
			String cKey = detector.WriteClusterKey(clusters.get(i));
			String cValue = detector.WriteClusterValue(clusters.get(i));
			context.write(new Text(cKey), new Text(cValue));
		}
	}
}