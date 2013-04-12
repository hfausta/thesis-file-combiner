import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FileCombiner {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	private static int numFiles;
	
	public static int getNumFiles() {
		return numFiles;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf();
    	conf.setNumMapTasks(1);
    	conf.setNumReduceTasks(5);
	
    	FileSystem fs = FileSystem.get(conf);
	    Path dir = new Path(args[0]);
	    FileStatus[] stats = fs.listStatus(dir);
	    numFiles = stats.length;
    	
		Job job = new Job(conf);
		job.setJarByClass(FileCombiner.class);
		job.setJobName("File Combiner");
		
		job.setMapperClass(FileCombinerMapper.class);
		job.setReducerClass(FileCombinerReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
