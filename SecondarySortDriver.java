import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

	
	public class SecondarySortDriver extends Configured implements Tool {
      public int run(String[] args) throws Exception {
          Configuration conf = getConf();
          Job job = Job.getInstance(conf);//new Job(conf);
          //job.setJarByClass(SecondarySortDriver.class);
          job.setJar("/home/kanika/myworkspace/SecondarySortMR.jar");
          job.setJobName("SecondarySortDriver");
 
          Path inputPath = new Path(args[0]);
          Path outputPath = new Path(args[1]);
          FileInputFormat.setInputPaths(job, inputPath);
          FileOutputFormat.setOutputPath(job, outputPath);
          job.setMapperClass(SecondarySortMapper.class);
          job.setMapOutputKeyClass(DateTemperaturePair.class);
          job.setMapOutputValueClass(DoubleWritable.class);
          job.setPartitionerClass(DateTemperaturePartitioner.class);
          job.setSortComparatorClass(DateTemperatureSortComparator.class);
          job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);
          job.setReducerClass(SecondarySortReducer.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(StringBuilder.class);

          boolean status = job.waitForCompletion(true);
          System.out.println("run(): status="+status);
          return status ? 0 : 1;
     }

     /**
     * The main driver for the secondary sort MapReduce program.
     * Invoke this method to submit the MapReduce job.
     * @throws Exception when there are communication
     * problems with the job tracker.
     */
     public static void main(String[] args) throws Exception {
         if (args.length != 2) {
             throw new IllegalArgumentException("Usage: SecondarySortDriver" +
                                                " <input-path> <output-path>");
         }
         int exitCode = ToolRunner.run(new SecondarySortDriver(), args);
         System.exit(exitCode);
     }

 }