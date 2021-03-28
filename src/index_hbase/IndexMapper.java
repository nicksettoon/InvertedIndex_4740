package index_hbase;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<Text, Text, NullWritable, Put> {

  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {
	  
	  byte[] columnFamily = Bytes.toBytes("page");
	  byte[] lineNum = Bytes.toBytes(key.toString());
	  byte[] valueOut = Bytes.toBytes("");
//		System.out.println("\nmapper start");
//		System.out.println(value);

	  for (String word : value.toString().split("\\W+"))
	  {
//		  System.out.println(word);
		  try{
			  Put line = new Put(Bytes.toBytes(word));
			  line.addImmutable(columnFamily, lineNum, valueOut);
			  context.write(null, line);
		  } catch (Exception IllegalArgumentException) {
			  continue;
		  }
	  }
  }
}