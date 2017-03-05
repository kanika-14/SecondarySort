
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
 
  public class DateTemperaturePartitioner
     extends Partitioner<DateTemperaturePair, NullWritable> {
 
      @Override
      public int getPartition(DateTemperaturePair pair, NullWritable value, int numberOfPartitions) {
    	  System.out.println("** Year Month in Comparator "+pair);
    	  return (pair.getYearMonth().hashCode() % numberOfPartitions);
      }
 }