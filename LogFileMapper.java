package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class LogFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String lines=value.toString();
	  for(String line:lines.split("\\n+")){//split the value by line
		  String IP=line.substring(0,line.indexOf("-"));//get the string before "-"
		  if(IP.length()>=10){ // avoid some blank lines being counted
			  context.write(new Text(IP),new IntWritable(1));
		  }
	  }
  }
}