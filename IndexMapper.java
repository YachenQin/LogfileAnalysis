package stubs;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
	  InputSplit inputSplit=context.getInputSplit();//use context to get inputpath
	  String fileName=((FileSplit)inputSplit).getPath().getName();//use getpath(),getname()function to get the filename
	  fileName.toLowerCase();
	  String lines = value.toString();
	  String line=lines.toLowerCase();//change all letter to small
	  String number=line.substring(0,line.indexOf("	"));//get the number before tab
	  String words=line.substring(line.indexOf("	")+1);//get the sentence after tab
	  for (String word : line.split("\\W+")) {
		  if(words.length()!=0){
			  context.write(new Text(word), new Text(fileName+"@"+number));
		  }
	  }
  }
}