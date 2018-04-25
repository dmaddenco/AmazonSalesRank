import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.math.BigInteger;


public class Driver {

  public static class CountersClass {
    public enum N_COUNTERS {
      SOMECOUNT
    }
  }

  private static class PartitionerAsin extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
      return Math.abs(key.toString().hashCode() % numReduceTasks);
    }
  }

  /**
   * Partition based on salesRank value
   *
   * @param Text key is salesRank
   * @param Text value is ProductTF-IDFvalue
   * @returns partition value based on salesRank
   */
  private static class SalesRankPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
      return Math.abs(key.toString().hashCode() % numReduceTasks);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", "\t");
    int numReduceTask = 32;

    //create all path variables
    Path stopWordsInputPath = new Path(args[1]);
    Path metaDataInputPath = new Path(args[2]);
    Path reviewDataInputPath = new Path(args[3]);
    Path outputPathTemp1 = new Path(args[4] + "Temp1");
    Path outputPathTemp2 = new Path(args[4] + "Temp2");
    Path outputPathTemp3 = new Path(args[4] + "Temp3");
    Path outputPathTemp4 = new Path(args[4] + "Temp4");
    Path outputPathTemp5 = new Path(args[4] + "Temp5");
    Path outputPathTemp6 = new Path(args[4] + "Temp6");
    Path outputPathTemp7 = new Path(args[4] + "Temp7");
    Path outputPath = new Path(args[4]);

    //create all job objects
    Job job1 = Job.getInstance(conf, "tp_job1");
    Job job2 = Job.getInstance(conf, "tp_job2");
    Job job3 = Job.getInstance(conf, "tp_job3");
    Job job4 = Job.getInstance(conf, "tp_job4");
    Job job5 = Job.getInstance(conf, "tp_job5");
    Job job6 = Job.getInstance(conf, "tp_job6");
    Job job7 = Job.getInstance(conf, "tp_job7");

    job1.setJarByClass(Driver.class);
    job1.setNumReduceTasks(numReduceTask);

    job1.setMapperClass(Job1.Job1Mapper.class);
    job1.setReducerClass(Job1.Job1Reducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job1, metaDataInputPath, TextInputFormat.class, Job1.Job0Mapper.class);
    MultipleInputs.addInputPath(job1, reviewDataInputPath, TextInputFormat.class, Job1.Job1Mapper.class);
    FileOutputFormat.setOutputPath(job1, outputPathTemp1);  //jobs write to intermediate output

    if (job1.waitForCompletion(true)) {
      job2.setJarByClass(Driver.class);
      job2.setNumReduceTasks(numReduceTask);

      job2.setMapperClass(Job2.Job2Mapper.class);
      job2.setReducerClass(Job2.Job2Reducer.class);

      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);

      FileSystem fs = FileSystem.get(conf);
      FileStatus[] fileList = fs.listStatus(stopWordsInputPath);

      for (FileStatus aFileList : fileList) {
        job2.addCacheFile(aFileList.getPath().toUri());
      }

      FileInputFormat.addInputPath(job2, outputPathTemp1);
      FileOutputFormat.setOutputPath(job2, outputPathTemp2);

      if (job2.waitForCompletion(true)) {

        job3.setJarByClass(Driver.class);
        job3.setNumReduceTasks(numReduceTask);

        job3.setMapperClass(Job3.Job3Mapper.class);
        job3.setReducerClass(Job3.Job3Reducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, outputPathTemp2);
        FileOutputFormat.setOutputPath(job3, outputPathTemp3);

        if (job3.waitForCompletion(true)) {
          Counter someCount = job3.getCounters().findCounter(CountersClass.N_COUNTERS.SOMECOUNT);

          job4.setJarByClass(Driver.class);
          job4.setNumReduceTasks(numReduceTask);

          job4.setMapperClass(Job4.Job4Mapper.class);
          job4.setReducerClass(Job4.Job4Reducer.class);

          job4.setMapOutputKeyClass(Text.class);
          job4.setMapOutputValueClass(Text.class);
          job4.setOutputKeyClass(Text.class);
          job4.setOutputValueClass(Text.class);

          FileInputFormat.addInputPath(job4, outputPathTemp3);
          FileOutputFormat.setOutputPath(job4, outputPathTemp4);

          if (job4.waitForCompletion(true)) {
            job5.getConfiguration().setLong(CountersClass.N_COUNTERS.SOMECOUNT.name(), someCount.getValue());

            job5.setJarByClass(Driver.class);
            job5.setNumReduceTasks(numReduceTask);

            job5.setMapperClass(Job5.Job5Mapper.class);
            job5.setReducerClass(Job5.Job5Reducer.class);

            job5.setMapOutputKeyClass(Text.class);
            job5.setMapOutputValueClass(Text.class);
            job5.setOutputKeyClass(Text.class);
            job5.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job5, outputPathTemp4);
            FileOutputFormat.setOutputPath(job5, outputPathTemp5);
            if (job5.waitForCompletion(true)) {

              job6.setJarByClass(Driver.class);
              job6.setNumReduceTasks(numReduceTask);
              job6.setPartitionerClass(SalesRankPartitioner.class);

              job6.setMapperClass(Job6.Job6Mapper.class);
              job6.setReducerClass(Job6.Job6Reducer.class);

              job6.setMapOutputKeyClass(Text.class);
              job6.setMapOutputValueClass(Text.class);
              job6.setOutputKeyClass(Text.class);
              job6.setOutputValueClass(Text.class);

              FileInputFormat.addInputPath(job6, outputPathTemp5);
              FileOutputFormat.setOutputPath(job6, outputPathTemp6);

//              System.exit(job6.waitForCompletion(true) ? 0 : 1);
              if (job6.waitForCompletion(true)) {

                job7.setJarByClass(Driver.class);
                job7.setNumReduceTasks(numReduceTask);
                job7.setPartitionerClass(SalesRankPartitioner.class);

                job7.setMapperClass(Job7.Job7Mapper.class);
                job7.setReducerClass(Job7.Job7Reducer.class);

                job7.setMapOutputKeyClass(Text.class);
                job7.setMapOutputValueClass(Text.class);
                job7.setOutputKeyClass(Text.class);
                job7.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job7, outputPathTemp6);
                FileOutputFormat.setOutputPath(job7, outputPathTemp7);

                System.exit(job7.waitForCompletion(true) ? 0 : 1);
              }
            }
          }
        }
      }
    }
  }
}
