import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<DateTemperaturePair, DoubleWritable, Text, StringBuilder>{
	   /**
	   * @param key is a DateTemperaturePair object
	   * @param value is a list of temperatures
	   */
	public void reduce(DateTemperaturePair key, Iterable<DoubleWritable> values, 
			Context context) throws IOException, InterruptedException {
		
			StringBuilder sortedTemperatureList = new StringBuilder();
			for (DoubleWritable temperature : values) {
				sortedTemperatureList.append(temperature);
				sortedTemperatureList.append(",");
			}
			sortedTemperatureList.deleteCharAt(sortedTemperatureList.length()-1);
			context.write(key.getYearMonth(), sortedTemperatureList);
	 }
}
