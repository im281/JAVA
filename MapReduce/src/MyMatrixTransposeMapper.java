import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMatrixTransposeMapper extends Mapper<LongWritable,Text,Text,Text>
{

	/**
	 * Map function will collect/ group cell values required for
	 * calculating the output.
	 * @param key is the row
	 * @param value is a single line. (a, 0, 0, 63) (matrix name, row, column, value)
	 *
	 */

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException
	{
		System.out.println("Inside Map !");
		String line = value.toString();
		String[] entry = line.split(",");
		String sKey = "";
		String row;

		Configuration conf = context.getConfiguration();
		String dimension = conf.get("dimension");

		System.out.println("Dimension from Mapper = " + dimension);
		row = entry[2].trim(); // rowid
		sKey = row;
		System.out.println(sKey + "-" + value.toString());
		context.write(new Text(sKey),value);


	}


}