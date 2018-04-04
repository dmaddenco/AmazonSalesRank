import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

class Job3 {
  /**
   * Map output from previous MapReduce job to new < key, value > pair
   * @param LongWritable object that can be ignored
   * @param Text object that contains all the output from previous MapReduce job
   * @return Key value pair < unigram, {docId \t TFvalue} >
   */
  static class Job3Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text unigramKey = new Text();
    private final Text compValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] inputArray = value.toString().split("\t");
      String docId = inputArray[0];
      String uni = inputArray[1];
      String freq = inputArray[2];
      String tf = inputArray[3];

      unigramKey.set(uni);
      compValue.set(docId + "\t" + tf);
      context.write(unigramKey, compValue);
    }
  }

  /**
   * Calculate IDF value
   * @param Text object key that is the unigram
   * @param Text object value that is composite value of {docId \t TFvalue}
   * @return Write to context the < key, value > pair of < unigram, {docId \t TFvalue \t Ni} >
   */
  static class Job3Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text unigramKey = new Text();
    private final Text compValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      ArrayList<String> valuesCopy = new ArrayList<String>();
      double ni;
      String tempValue;

      //create deep copy of values
      for (Text val : values) {
        valuesCopy.add(val.toString());
      }

      //number of documents where unigram exists
      ni = valuesCopy.size();

      //iterate over ArrayList of value copies
      for (String val : valuesCopy) {
        String[] inputArray = val.split("\t");
        String docId = inputArray[0];
        String tf = inputArray[1];
        tempValue = docId + "\t" + tf + "\t" + ni;
        unigramKey.set(key.toString());
        compValue.set(tempValue);
        context.write(unigramKey, compValue);
      }
    }
  }
}
