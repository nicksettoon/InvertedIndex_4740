package index_hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

public class InvertedIndex extends Configured implements Tool {

  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: InvertedIndex <input dir> <output dir>\n");
      return -1;
    }

    // job setup
    PropertyConfigurator.configure("log4j.properties");
    Job job = Job.getInstance(getConf(), "Inverted Index");
    job.setJarByClass(InvertedIndex.class);
    job.setJobName("Inverted Index");
    
    //input setup
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    job.setInputFormatClass(KeyValueTextInputFormat.class);

    // mapper setup
    job.setNumReduceTasks(0);
    job.setMapperClass(IndexMapper.class);

    // output setup
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setOutputFormatClass(TableOutputFormat.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "test2");
    

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
    System.exit(exitCode);
  }
}
