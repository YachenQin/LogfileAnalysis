package stubs;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class IndexReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  String info = "";
	  for (Text value:values){
		  if(info.length()==0){
			  info=value.toString();
		  }
		  else{
			  info=value.toString();
			  info=info+","; //Separate the value of same key with ','  
		  }
	  }
	  context.write(key, new Text(info));
  }
}