package componentDetection;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class AlignmentReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		System.out.println("Inside Alignment reducer !");
		String fValue = "";
		double referenceRT = 0;
		double rtOffset = 0;

		// search for the reference file to set reference RT
		for (Text val : values) {
			String line = val.toString();
			String[] elements = line.split("[,]");

			if (elements[0].contains("1")) {
				referenceRT = Double.parseDouble(elements[4]);
				rtOffset = 0;
				fValue = line + "," + rtOffset;
				context.write(key, new Text(fValue));
				break;
			}
		}
		if (referenceRT != 0) {
			for (Text val : values) {
				String line = val.toString();
				String[] elements = line.split("[,]");

				if (elements.toString().contains("1")) {
					referenceRT = Double.parseDouble(elements[4]);
					rtOffset = 0;
				} else {
					rtOffset = referenceRT - Double.parseDouble(elements[4]);
				}

				fValue = line + "," + rtOffset;
				// Write out features with corrected RT values
				// cValue = val.toString();
				context.write(key, new Text(fValue));
			}
		}
	}
}