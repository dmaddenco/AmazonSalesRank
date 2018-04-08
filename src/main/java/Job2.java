import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

class Job2 {
  /**
   * Map results from first MapReduce job to new < key, value > pair
   *
   * @param LongWritable object that can be ignored
   * @param Text         object that contains all output from previous MapReduce job < asin, {customerReviews, salesRank} >
   * @return Key value pair < {asin, unigram}, {1, salesRank} >
   */
  static class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text compKey = new Text();
    private final Text compValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] inputArray = value.toString().split("\t");
      if (inputArray.length >= 3) {
        String asin = inputArray[0];
        String reviews = inputArray[1];
        String salesRank = inputArray[2];
        String[] sentences = reviews.split("\\.");

        for (String sentence : sentences) {
          if (!sentence.equals("")) {
            StringTokenizer itrWord = new StringTokenizer(sentence);

            while (itrWord.hasMoreTokens()) {
              String unigram = itrWord.nextToken().toLowerCase().replaceAll("[^a-z0-9 ]", "");
              compKey.set(asin + "\t" + unigram);
              compValue.set(1 + "\t" + salesRank);
              context.write(compKey, compValue);
            }
          }
        }
      }
    }
  }

  /**
   * Sum up all counts for unique key, in this case a unigram
   *
   * @param Text composite key containing {asin, unigram}
   * @param Text composite value containing {1, salesRank}
   * @return Write to context the < key, value > pair of < {asin, unigram}, {frequency, salesRank} >
   */
  static class Job2Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text compKey = new Text();
    private final Text compValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      String salesRank= "";

      for (Text val : values) {
        String[] valArr = val.toString().split("\t");
        sum += Integer.parseInt(valArr[0]);
        salesRank = valArr[1];
      }

      compKey.set(key);
      compValue.set(sum + "\t" + salesRank);
      context.write(compKey, compValue);
    }
  }
}
