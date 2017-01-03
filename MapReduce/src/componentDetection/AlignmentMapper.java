package componentDetection;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class AlignmentMapper extends Mapper<LongWritable, 
Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("Inside Alignment Map !");
		String line = value.toString();
		String[] elements = line.split("['\t'_']");
		
		//send clusters to reducer by key and value
		context.write(new Text(elements[0]), new Text(elements[2]));
		
	}
}
}