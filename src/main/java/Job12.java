import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class Job12 {

  /**
   * Map output from previous MapReduce job of < unigram, {asin \t TFValue \t ni} >
   * to new < key, value > pair and calculate IDF and TF-IDF values
   *
   * @param LongWritable object that can be ignored
   * @param Text         object that contains all the output from previous MapReduce job
   * @return Key value pair < asin, {unigram \t TFvalue \t TF-IDFvalue} >
   */
  static class Job12Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text asinKey = new Text();
    private final Text compValue = new Text();

    private long someCount;

    /**
     * Get count value from context and set equal to local variable
     *
     * @param context contains count value from driver
     */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
      super.setup(context);
      this.someCount = context.getConfiguration().getLong(Driver.CountersClass.N_COUNTERS.SOMECOUNT.name(), 0);
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      double idf, N, tfidf;

      String[] values = value.toString().split("\t");
      String unigram = values[0];
      String asin = values[1];
      double tf = Double.parseDouble(values[2]);
      double ni = Double.parseDouble(values[3]);

      N = this.someCount;
      idf = Math.log10(N / ni);
      tfidf = tf * idf;

      asinKey.set(asin);
      compValue.set(unigram + "\t" + tf + "\t" + tfidf);
      context.write(asinKey, compValue);
    }
  }

  /**
   * Identity reducer
   *
   * @param Text object key that is the asin
   * @param Text object value that is the composite key of {unigram \t TFvalue \t TF-IDFvalue}
   * @return Key value pair < asin, {unigram \t TFvalue \t TF-IDFvalue} >
   */
  static class Job12Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text asin = new Text();
    private final Text compValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      asin.set(key);

      for (Text val : values) {
        compValue.set(val);
        context.write(asin, compValue);
      }
    }
  }
}
