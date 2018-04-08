import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Driver {

  public static class CountersClass {
    public enum N_COUNTERS {
      SOMECOUNT
    }
  }

  /**
   * Partitions based on document id and number of reduce tasks
   *
   * @param DocIdUniComKey is the composite key that contains {docId \t unigram}
   * @param IntWritable is the value for the key that can be ignored
   * @return Modulo result between the document id and the number of reduce tasks set in driver
   */
  private static class PartitionerInitial extends Partitioner<DocIdUniComKey, IntWritable> {
    public int getPartition(DocIdUniComKey key, IntWritable value, int numReduceTasks) {
      return Math.abs(key.getDocID().hashCode() % numReduceTasks);
    }
  }
  
  public static class Partitioner0 extends Partitioner<Text,Text>{
            @Override
            public int getPartition(Text key, Text value, int numReduceTasks){
                System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!GOT HERE");
                System.out.println("*******************************************KEY:" + key.toString() + " VALUE: " + value.toString());
                int val = Integer.parseInt(key.toString()) % numReduceTasks;
                //System.out.println("*******************************************KEY:" + key.toString() + " VALUE: " + val);
                return val;
            }
        }

  /**
   * Creates composite key: {docId \t unigram}
   */
  public static class DocIdUniComKey implements Writable, WritableComparable<DocIdUniComKey> {
    private IntWritable docID = new IntWritable();
    private Text unigram = new Text();

    DocIdUniComKey() {
      this.docID = new IntWritable();
      this.unigram = new Text();
    }

    DocIdUniComKey(IntWritable id, Text uni) {
      this.docID.set(Integer.parseInt(id.toString()));
      this.unigram.set(uni);
    }

    public void write(DataOutput out) throws IOException {
      this.docID.write(out);
      this.unigram.write(out);
    }

    public void readFields(DataInput in) throws IOException {
      this.docID = new IntWritable();
      this.unigram = new Text();
      this.docID.readFields(in);
      this.unigram.readFields(in);
    }

    IntWritable getDocID() {
      return this.docID;
    }

    Text getUnigram() {
      return this.unigram;
    }

    public int compareTo(DocIdUniComKey pair) {
      int compareValue = this.docID.compareTo(pair.getDocID());
      if (compareValue == 0) {
        compareValue = unigram.compareTo(pair.getUnigram());
      }
      return -1 * compareValue; //descending order
    }

    @Override
    public String toString() {
      return docID.toString() + "\t" + unigram.toString();
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", "\t");
    int numReduceTask = 32;

    //create all path variables
    Path inputPath = new Path(args[0]);
    Path outputPathTemp0 = new Path(args[1] + "Temp0");
    Path outputPathTemp1 = new Path(args[1] + "Temp1");
    Path outputPathTemp2 = new Path(args[1] + "Temp2");
    Path outputPathTemp3 = new Path(args[1] + "Temp3");
    Path outputPathTemp4 = new Path(args[1] + "Temp4");
    Path outputPath = new Path(args[1]);

    //create all job objects
    Job job0 = Job.getInstance(conf, "tp_job0");
    Job job1 = Job.getInstance(conf, "tp_job1");
    Job job2 = Job.getInstance(conf, "tp_job2");
    Job job3 = Job.getInstance(conf, "tp_job3");
    Job job4 = Job.getInstance(conf, "tp_job4");
    Job job5 = Job.getInstance(conf, "tp_job5");
    
    job0.setJarByClass(Driver.class);
    job0.setNumReduceTasks(numReduceTask);
    //job0.setPartitionerClass(Partitioner0.class);

    job0.setMapperClass(Job0.Job0Mapper.class);
    job0.setReducerClass(Job0.Job0Reducer.class);
    job0.setOutputKeyClass(Text.class);
    job0.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job0, inputPath);
    FileOutputFormat.setOutputPath(job0, outputPathTemp0);  //jobs write to intermediate output
    
    System.exit(job0.waitForCompletion(true) ? 0 : 1);
    /*
    if (job0.waitForCompletion(true)) {
    job1.setJarByClass(Driver.class);
    job1.setNumReduceTasks(numReduceTask);
    job1.setPartitionerClass(PartitionerInitial.class);

    job1.setMapperClass(Job1.Job1Mapper.class);
    job1.setReducerClass(Job1.Job1Reducer.class);
    job1.setOutputKeyClass(DocIdUniComKey.class);
    job1.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, outputPathTemp1);  //jobs write to intermediate output

    if (job1.waitForCompletion(true)) {
      job2.setJarByClass(Driver.class);
      job2.setNumReduceTasks(numReduceTask);

      job2.setMapperClass(Job2.Job2Mapper.class);
      job2.setReducerClass(Job2.Job2Reducer.class);

      job2.setMapOutputKeyClass(IntWritable.class);
      job2.setMapOutputValueClass(Text.class);
      job2.setOutputKeyClass(IntWritable.class);
      job2.setOutputValueClass(Text.class);

      FileInputFormat.addInputPath(job2, outputPathTemp1);
      FileOutputFormat.setOutputPath(job2, outputPathTemp2);

      if (job2.waitForCompletion(true)) {
        //create counter to keep track of number of documents
        Counter count = job2.getCounters().findCounter(CountersClass.N_COUNTERS.SOMECOUNT);

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
          //set job4's counter equal to the value of job3's counter
          job4.getConfiguration().setLong(CountersClass.N_COUNTERS.SOMECOUNT.name(), count.getValue());

          job4.setJarByClass(Driver.class);
          job4.setNumReduceTasks(numReduceTask);

          job4.setMapperClass(Job4.Job4Mapper.class);
          job4.setReducerClass(Job4.Job4Reducer.class);

          job4.setMapOutputKeyClass(IntWritable.class);
          job4.setMapOutputValueClass(Text.class);
          job4.setOutputKeyClass(IntWritable.class);
          job4.setOutputValueClass(Text.class);

          FileInputFormat.addInputPath(job4, outputPathTemp3);
          FileOutputFormat.setOutputPath(job4, outputPathTemp4);

          if (job4.waitForCompletion(true)) {
            //TODO: Remove distributed cache and use instead MultipleInputs.addInputPath()
            FileSystem fs = FileSystem.get(conf);
            //only get file paths that start with "part-r"
            FileStatus[] fileList = fs.listStatus((outputPathTemp4),
                    new PathFilter() {
                      public boolean accept(Path path) {
                        return path.getName().startsWith("part-");
                      }
                    });
            //adding files to distributed cache
            for (FileStatus aFileList : fileList) {
              job5.addCacheFile((aFileList.getPath().toUri()));
            }

            job5.setJarByClass(Driver.class);
            job5.setNumReduceTasks(numReduceTask);

            job5.setMapperClass(Job5.Job5Mapper.class);
            job5.setReducerClass(Job5.Job5Reducer.class);

            job5.setMapOutputKeyClass(IntWritable.class);
            job5.setMapOutputValueClass(Text.class);
            job5.setOutputKeyClass(IntWritable.class);
            job5.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job5, inputPath);
            FileOutputFormat.setOutputPath(job5, outputPath);
            System.exit(job5.waitForCompletion(true) ? 0 : 1);
          }
        }
      }
    }
   } */
  }
}
