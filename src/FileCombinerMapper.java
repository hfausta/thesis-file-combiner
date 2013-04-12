import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileCombinerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String fileName = fileSplit.getPath().getName().trim();
		String[] fileNameSplit = fileName.split("part-r-");
		
		int outKey = Integer.parseInt(fileNameSplit[1]);
		int numFiles = FileCombiner.getNumFiles() - 1;
		if(outKey != numFiles) {
			context.write(new IntWritable(outKey), value);
		}
		if(outKey > 0) {
			context.write(new IntWritable(outKey-1), value);
		}
	}
}
