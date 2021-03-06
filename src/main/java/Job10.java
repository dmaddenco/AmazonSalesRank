import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

class Job10 {
  /**
   * Map output from previous MapReduce job < {asin \t unigram}, frequency > to new < key, value > pair
   *
   * @param LongWritable object that can be ignored
   * @param Text         object that contains all the output from previous MapReduce job
   * @return Key value pair < asin, {unigram \t frequency} >
   */
  static class Job10Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text compKey = new Text();
    private final Text compValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      Set<String> uniqueASIN = new HashSet<String>();
      String[] valueArray = value.toString().split("\t");

      String asin = valueArray[0];
      String unigram = valueArray[1];
      String frequency = valueArray[2];

      compKey.set(asin);
      compValue.set(unigram + "\t" + frequency);
      context.write(compKey, compValue);

      if (!uniqueASIN.contains(asin)) {
        uniqueASIN.add(asin);
        context.getCounter(Driver.CountersClass.N_COUNTERS.SOMECOUNT).increment(1); //Increment the counter
      }
    }
  }

  /**
   * Calculate TF value
   *
   * @param Text object key that is the asin
   * @param Text object value that is composite value of {unigram \t frequency}
   * @return Write to context the < key, value > pair of < asin, {unigram \t TFvalue} >
   */
  static class Job10Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text compKey = new Text();
    private final Text compValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      ArrayList<String> valuesCopy = new ArrayList<String>();
      double maxFreq = 0;
      double tf;

      for (Text val : values) {
        valuesCopy.add(val.toString());
      }

      for (String val : valuesCopy) {
        String[] valuesSplit = val.split("\t");
        int frequency = Integer.parseInt(valuesSplit[1]);
        if (frequency > maxFreq) {
          maxFreq = frequency;
        }
      }

      for (String val : valuesCopy) {
        String[] valuesSplit = val.split("\t");
        String unigram = valuesSplit[0];
        int frequency = Integer.parseInt(valuesSplit[1]);
        tf = 0.5 + 0.5 * (frequency / maxFreq);
        compKey.set(key);
        compValue.set(unigram + "\t" + tf);
        context.write(compKey, compValue);
      }
    }
  }
}
