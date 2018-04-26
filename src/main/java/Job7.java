import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class Job7 {

  /**
   * Identity mapper that creates dummy key that represents partition
   *
   * @param LongWritable object that can be ignored
   * @param Text         object that contains all the output from previous MapReduce job
   * @return Key value pair < dummyKey, {salesRank \t TFIDF} >
   */
  static class Job7Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text partKey = new Text();
    private final Text comKey = new Text();

    private long numReduceTasks;

    /**
     * Get number of reducers from context and set equal to local variable
     *
     * @param context contains count value from driver
     */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
      super.setup(context);
      this.numReduceTasks = context.getConfiguration().getLong(Driver.CountersClass.N_COUNTERS.SOMECOUNT.name(), 0);
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      String[] values = value.toString().split("\t");
      String rank = values[0];
      String tfidf = values[1];

      double dummyKey = Math.abs(rank.hashCode() % numReduceTasks);

      partKey.set(Double.toString(dummyKey));
      comKey.set(rank + "\t" + tfidf);
      context.write(partKey, comKey);
    }
  }

  static class Job7Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text maxValues = new Text();
    private final Text minValues = new Text();


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      long maxRank = 0;
      long minRank = Long.MAX_VALUE;
      double maxtfidf = 0;
      double mintfidf = Double.MAX_VALUE;

      for (Text val : values) {
        String[] valueSplit = val.toString().split("\t");
        long tempRank = Long.parseLong(valueSplit[0]);
        double tempTFIDF = Double.parseDouble(valueSplit[1]);

        if (tempRank > maxRank) {
          maxRank = tempRank;
        }
        if (tempRank < minRank) {
          minRank = tempRank;
        }
        if (tempTFIDF > maxtfidf) {
          maxtfidf = tempTFIDF;
        }
        if (tempTFIDF < mintfidf) {
          mintfidf = tempTFIDF;
        }
      }

      maxValues.set(Double.toString(maxRank) + "\t" + Double.toString(maxtfidf));
      minValues.set(Double.toString(minRank) + "\t" + Double.toString(mintfidf));
      context.write(maxValues, minValues);
    }
  }
}
