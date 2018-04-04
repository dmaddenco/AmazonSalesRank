import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

class Job2 {
  /**
   * Map results from first MapReduce job to new < key, value > pair
   * @param LongWritable object that can be ignored
   * @param Text object that contains all the output from first MapReduce job
   * @return Key value pair < docId, {unigram \t frequecy} >
   */
  static class Job2Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private final IntWritable docId = new IntWritable();
    private final Text compValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] inputArray = value.toString().split("\t");
      String id = inputArray[0];
      String uni = inputArray[1];
      String freq = inputArray[2];
      docId.set(Integer.parseInt(id));
      compValue.set(uni + "\t" + freq);
      context.write(docId, compValue);
    }
  }

  /**
   * Calculate TF value for unigram using formula TFij = 0.5 + 0.5 (Fij / MAXk(Fkj))
   * Count the number of unique documents using docId and Counter object
   * @param IntWritable docId key
   * @param Text composite value containing {unigram \t frequency}
   * @return Write to context the < key, value > pair of < docId, {unigram \t frequency \t TFvalue} >
   */
  static class Job2Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private final IntWritable docId = new IntWritable();
    private final Text compValue = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      ArrayList<String> valuesCopy = new ArrayList<String>();
      Set<String> uniqueIDs = new HashSet<String>();
      double maxFreq = 0;
      double tf;
      String tempValue;

      //create deep copy of values
      for (Text val : values) {
        valuesCopy.add(val.toString());
      }

      //iterate over ArrayList that contains copy of values
      //find max frequency
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
        tempValue = unigram + "\t" + frequency + "\t" + tf;
        compValue.set(tempValue);
        docId.set(Integer.parseInt(key.toString()));
        context.write(docId, compValue);
      }

      //count number of unique documents using docId
      if (!uniqueIDs.contains(key.toString())){
        uniqueIDs.add(key.toString());
        context.getCounter(Driver.CountersClass.N_COUNTERS.SOMECOUNT).increment(1); //Increment the counter
      }
    }
  }
}
