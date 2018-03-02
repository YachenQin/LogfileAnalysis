package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private String pattern="[^a-zA-Z-']";//all special characters
  
	@Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String line = value.toString();
	  line=line.replaceAll(pattern," ");//replace all special characters with blank space
	  line=line.toLowerCase();//change all letters to low case
	  String word1=null;
	  for(String word:line.split("\\W+")){
		  if(word.length()>0){
			  if(word1 !=null){
				  context.write(new Text(word1+","+word),new IntWritable(1));
			  }
			  word1=word;//create word pairs
		  }
	  }
	  
  }
}
