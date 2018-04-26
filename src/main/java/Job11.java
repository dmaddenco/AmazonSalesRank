import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

class Job11 {
  /**
   * Map output from previous MapReduce job of < asin, {unigram /t TFValue} >
   * to new < key, value > pair and calculate IDF value
   *
   * @param LongWritable object that can be ignored
   * @param Text         object that contains all the output from previous MapReduce job
   * @return Key value pair < unigram, {asin \t TFValue} >
   */
  static class Job11Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text unigramKey = new Text();
    private final Text compValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] inputArray = value.toString().split("\t");
      String asin = inputArray[0];
      String unigram = inputArray[1];
      String tf = inputArray[2];

      unigramKey.set(unigram);
      compValue.set(asin + "\t" + tf);
      context.write(unigramKey, compValue);
    }
  }

  /**
   * Identity reducer
   *
   * @param Text object key that is the unigram
   * @param Text object value that is the composite value of {asin \t TFValue}
   * @return Write to context key value pair < unigram, {asin \t TFValue \t ni} >
   */
  static class Job11Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text unigramKey = new Text();
    private final Text compValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      ArrayList<String> valuesCopy = new ArrayList<String>();
      double ni;

      for (Text val : values) {
        valuesCopy.add(val.toString());
      }

      ni = valuesCopy.size();

      for (String val : valuesCopy) {
        String[] inputArray = val.split("\t");
        String asin = inputArray[0];
        String tf = inputArray[1];
        unigramKey.set(key.toString());
        compValue.set(asin + "\t" + tf + "\t" + ni);
        context.write(unigramKey, compValue);
      }
    }
  }
}
