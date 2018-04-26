import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


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

  private static class PartitionerAsinInt extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
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
      String[] valueParts = value.toString().split("\t");
      return Math.abs(valueParts[0].hashCode() % numReduceTasks);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", "\t");
    int numReduceTask = 32;

    //create all path variables
    Path stopWordsInputPath = new Path(args[1]);
    Path metaDataInputPathTraining = new Path(args[2]);
    Path reviewDataInputPathTraining = new Path(args[3]);
    Path outputPathTemp1 = new Path(args[4] + "Temp1");
    Path outputPathTemp2 = new Path(args[4] + "Temp2");
    Path outputPathTemp3 = new Path(args[4] + "Temp3");
    Path outputPathTemp4 = new Path(args[4] + "Temp4");
    Path outputPathTemp5 = new Path(args[4] + "Temp5");
    Path outputPathTemp6 = new Path(args[4] + "Temp6");
    Path outputPathTraining = new Path(args[4]);

    Path reviewDataInputPathTesting = new Path(args[5]);
    Path outputPathTemp8 = new Path(args[6] + "Temp8");
    Path outputPathTemp9 = new Path(args[6] + "Temp9");
    Path outputPathTemp10 = new Path(args[6] + "Temp10");
    Path outputPathTemp11 = new Path(args[6] + "Temp11");
    Path outputPathTemp12 = new Path(args[6] + "Temp12");
    Path outputPathTemp13 = new Path(args[6] + "Temp13");
    Path outputPathTesting = new Path(args[6]);


    //create all job objects
    Job job1 = Job.getInstance(conf, "tp_job1");
    Job job2 = Job.getInstance(conf, "tp_job2");
    Job job3 = Job.getInstance(conf, "tp_job3");
    Job job4 = Job.getInstance(conf, "tp_job4");
    Job job5 = Job.getInstance(conf, "tp_job5");
    Job job6 = Job.getInstance(conf, "tp_job6");
    Job job7 = Job.getInstance(conf, "tp_job7");
    Job job8 = Job.getInstance(conf, "tp_job8");
    Job job9 = Job.getInstance(conf, "tp_job9");
    Job job10 = Job.getInstance(conf, "tp_job10");
    Job job11 = Job.getInstance(conf, "tp_job11");
    Job job12 = Job.getInstance(conf, "tp_job12");
    Job job13 = Job.getInstance(conf, "tp_job13");
    Job job14 = Job.getInstance(conf, "tp_job14");

    job1.setJarByClass(Driver.class);
    job1.setNumReduceTasks(numReduceTask);

    job1.setMapperClass(Job1.Job1Mapper.class);
    job1.setReducerClass(Job1.Job1Reducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job1, metaDataInputPathTraining, TextInputFormat.class, Job1.Job0Mapper.class);
    MultipleInputs.addInputPath(job1, reviewDataInputPathTraining, TextInputFormat.class, Job1.Job1Mapper.class);
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
//              job6.setPartitionerClass(SalesRankPartitioner.class);

              job6.setMapperClass(Job6.Job6Mapper.class);
              job6.setReducerClass(Job6.Job6Reducer.class);

              job6.setMapOutputKeyClass(Text.class);
              job6.setMapOutputValueClass(Text.class);
              job6.setOutputKeyClass(Text.class);
              job6.setOutputValueClass(Text.class);

              FileInputFormat.addInputPath(job6, outputPathTemp5);
              FileOutputFormat.setOutputPath(job6, outputPathTemp6);

              if (job6.waitForCompletion(true)) {
                job7.getConfiguration().setLong(CountersClass.N_COUNTERS.SOMECOUNT.name(), numReduceTask);

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
                FileOutputFormat.setOutputPath(job7, outputPathTraining);

                if (job7.waitForCompletion(true)) {

                  job8.setJarByClass(Driver.class);
                  job8.setNumReduceTasks(numReduceTask);
//                  job8.setPartitionerClass(PartitionerAsin.class);

                  job8.setMapperClass(Job8.Job8Mapper.class);
                  job8.setReducerClass(Job8.Job8Reducer.class);

                  job8.setOutputKeyClass(Text.class);
                  job8.setOutputValueClass(Text.class);

                  FileInputFormat.addInputPath(job8, reviewDataInputPathTesting);
                  FileOutputFormat.setOutputPath(job8, outputPathTemp8);

                  if (job8.waitForCompletion(true)) {

                    job9.setJarByClass(Driver.class);
                    job9.setNumReduceTasks(numReduceTask);
//                    job9.setPartitionerClass(PartitionerAsinInt.class);

                    job9.setMapperClass(Job9.Job9Mapper.class);
                    job9.setReducerClass(Job9.Job9Reducer.class);

                    job9.setOutputKeyClass(Text.class);
                    job9.setOutputValueClass(IntWritable.class);

                    FileSystem fs1 = FileSystem.get(conf);
                    FileStatus[] fileList1 = fs1.listStatus(stopWordsInputPath);

                    for (FileStatus aFileList : fileList1) {
                      job9.addCacheFile(aFileList.getPath().toUri());
                    }

                    FileInputFormat.addInputPath(job9, outputPathTemp8);
                    FileOutputFormat.setOutputPath(job9, outputPathTemp9);

                    if (job9.waitForCompletion(true)) {

                      job10.setJarByClass(Driver.class);
                      job10.setNumReduceTasks(numReduceTask);

                      job10.setMapperClass(Job10.Job10Mapper.class);
                      job10.setReducerClass(Job10.Job10Reducer.class);

                      job10.setMapOutputKeyClass(Text.class);
                      job10.setMapOutputValueClass(Text.class);
                      job10.setOutputKeyClass(Text.class);
                      job10.setOutputValueClass(Text.class);

                      FileInputFormat.addInputPath(job10, outputPathTemp9);
                      FileOutputFormat.setOutputPath(job10, outputPathTemp10);

                      if (job10.waitForCompletion(true)) {
                        Counter someCountTesting = job10.getCounters().findCounter(CountersClass.N_COUNTERS.SOMECOUNT);

                        job11.setJarByClass(Driver.class);
                        job11.setNumReduceTasks(numReduceTask);

                        job11.setMapperClass(Job11.Job11Mapper.class);
                        job11.setReducerClass(Job11.Job11Reducer.class);

                        job11.setMapOutputKeyClass(Text.class);
                        job11.setMapOutputValueClass(Text.class);
                        job11.setOutputKeyClass(Text.class);
                        job11.setOutputValueClass(Text.class);

                        FileInputFormat.addInputPath(job11, outputPathTemp10);
                        FileOutputFormat.setOutputPath(job11, outputPathTemp11);

                        if (job11.waitForCompletion(true)) {
                          job12.getConfiguration().setLong(CountersClass.N_COUNTERS.SOMECOUNT.name(), someCountTesting.getValue());

                          job12.setJarByClass(Driver.class);
                          job12.setNumReduceTasks(numReduceTask);

                          job12.setMapperClass(Job12.Job12Mapper.class);
                          job12.setReducerClass(Job12.Job12Reducer.class);

                          job12.setMapOutputKeyClass(Text.class);
                          job12.setMapOutputValueClass(Text.class);
                          job12.setOutputKeyClass(Text.class);
                          job12.setOutputValueClass(Text.class);


                          FileInputFormat.addInputPath(job12, outputPathTemp11);
                          FileOutputFormat.setOutputPath(job12, outputPathTemp12);

                          if (job12.waitForCompletion(true)) {

                            job13.setJarByClass(Driver.class);
                            job13.setNumReduceTasks(numReduceTask);
//                            job13.setPartitionerClass(SalesRankPartitioner.class);

                            job13.setMapperClass(Job13.Job13Mapper.class);
                            job13.setReducerClass(Job13.Job13Reducer.class);

                            job13.setMapOutputKeyClass(Text.class);
                            job13.setMapOutputValueClass(Text.class);
                            job13.setOutputKeyClass(Text.class);
                            job13.setOutputValueClass(Text.class);

                            FileInputFormat.addInputPath(job13, outputPathTemp12);
                            FileOutputFormat.setOutputPath(job13, outputPathTemp13);

                            if (job13.waitForCompletion(true)) {

                              job14.setJarByClass(Driver.class);
                              job14.setNumReduceTasks(numReduceTask);

                              job14.setMapperClass(Job14.Job14Mapper.class);
                              job14.setReducerClass(Job14.Job14Reducer.class);

                              job14.setMapOutputKeyClass(Text.class);
                              job14.setMapOutputValueClass(Text.class);
                              job14.setOutputKeyClass(Text.class);
                              job14.setOutputValueClass(Text.class);

                              FileSystem FS = FileSystem.get(conf);
                              FileStatus[] fileList2 = FS.listStatus(outputPathTraining);

                              for (FileStatus aFileList : fileList2) {
                                job14.addCacheFile(aFileList.getPath().toUri());
                              }

                              FileInputFormat.addInputPath(job14, outputPathTemp13);
                              FileOutputFormat.setOutputPath(job14, outputPathTesting);

                              System.exit(job14.waitForCompletion(true) ? 0 : 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}