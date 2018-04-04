import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

class Job1 {
  /**
   * Maps text read from input file to < key, value > pairs of < {unigram \t frequecy}, 1 >
   * @param Object is the dummy key created when reading file
   * @param Text contains string objects that are from the input file
   * @return Writes to context the < DocIdUniComKey, 1 > key value pair
   */
  static class Job1Mapper extends Mapper<Object, Text, Driver.DocIdUniComKey, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    private final IntWritable docID = new IntWritable();
    private Driver.DocIdUniComKey comKey = new Driver.DocIdUniComKey();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String values[] = value.toString().split("<====>");

      if (values.length >= 3) {
        String id = values[1];
        String article = values[2];
        String[] sentences = article.split("\\.");

        for (String sentence : sentences) {
          if (!sentence.equals("")) {
            StringTokenizer itrWord = new StringTokenizer(sentence);

            while (itrWord.hasMoreTokens()) {
              String unigram = itrWord.nextToken().toLowerCase().replaceAll("[^a-z0-9 ]", "");
              word.set(unigram);
              docID.set(Integer.parseInt(id));
              comKey = new Driver.DocIdUniComKey(docID, word);
              context.write(comKey, one);
            }

          }
        }
      }
    }
  }

  /**
   * Sums up all counts for unique key, in this case a unigram
   * @param DocIdUniComKey unique composite key that contains the {docId, unigram}
   * @param IntWritable value of 1, for 1 occurrence for the composite key
   * @return Writes to context the frequency of a unigram < unigram, frequency >
   */
  static class Job1Reducer extends Reducer<Driver.DocIdUniComKey, IntWritable, Driver.DocIdUniComKey, IntWritable> {
    private final IntWritable result = new IntWritable();

    public void reduce(Driver.DocIdUniComKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;

      for (IntWritable val : values) {
        sum += val.get();
      }

      result.set(sum);
      context.write(key, result);
    }
  }
}
