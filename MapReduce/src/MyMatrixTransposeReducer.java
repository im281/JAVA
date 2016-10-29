import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyMatrixTransposeReducer extends Reducer<Text, Text, Text, Text>
{

    /**
     * Reducer do the actual transposition.
     * @param key is the row
     * @value same as input except swapped row and column in the value .
     */

    @Override
    protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException
    {

        Configuration conf = context.getConfiguration();
        String dimension = conf.get("dimension");

        int dim = Integer.parseInt(dimension);
     
        for(Text val : values)
        {
            String[] entries = val.toString().split(",");                                 	
            //swap row and column
            String row = entries[3]; 
            String col= entries[2]; 
            entries[2] =  row;  
            entries[3] =  col;     
                            
            //System.out.println(key.toString() + "-" + entries.toString());
            //context.write(new Text(row), new Text(ConvertToString(entries)));
            context.write(null, new Text(ConvertToString(entries)));
   
        }
    }
                             
    public String ConvertToString(String[] values) {
        String line = values[0];
           for (int i = 1; i < values.length; i++) {
            line = line + ","+ values[i].toString();
          }
         return line;
       }

}