import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.StringTokenizer;

class Job9 {
  private static final HashSet<String> stopWords = new HashSet<String>();

  /**
   * Map results from first MapReduce job to new < key, value > pair
   *
   * @param LongWritable object that can be ignored
   * @param Text         object that contains all output from previous MapReduce job < asin, customerReviews >
   * @return Key value pair < {asin, unigram}, 1 >
   */
  static class Job9Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text compKey = new Text();
    private final IntWritable count = new IntWritable();

    /**
     * Read in from multiple inputs and store results in memory
     * File input contains stop words to be used as a filter in mapper
     *
     * @param Context object contains file paths that were created in Driver.java
     */
    @Override
    public void setup(Context context) throws IOException {
      URI[] cacheFiles = context.getCacheFiles();
      if (cacheFiles != null && cacheFiles.length > 0) {
        try {
          BufferedReader reader = null;
          for (URI cacheFile1 : cacheFiles) {
            try {
              File cacheFile = new File(cacheFile1.getPath());

              reader = new BufferedReader(new FileReader(cacheFile.getName()));
              String line;

              while ((line = reader.readLine()) != null) {
                line = line.toLowerCase().replaceAll("[^a-z0-9 ]", "");
                stopWords.add(line);
              }
            } catch (IOException e) {
              e.printStackTrace();
            } finally {
              try {
                reader.close();
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] inputArray = value.toString().split("\t");

      if (inputArray.length == 2) {
        String asin = inputArray[0];
        String reviews = inputArray[1];
        String[] sentences = reviews.split("\\.");

        for (String sentence : sentences) {
          if (!sentence.equals("")) {
            StringTokenizer itrWord = new StringTokenizer(sentence);

            while (itrWord.hasMoreTokens()) {
              String unigram = itrWord.nextToken().toLowerCase().replaceAll("[^a-z0-9 ]", "");

              if (!stopWords.contains(unigram)) {
                compKey.set(asin + "\t" + unigram);
                count.set(1);
                context.write(compKey, count);
              }
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
   * @param Text composite value containing 1
   * @return Write to context the < key, value > pair of < {asin, unigram}, frequency >
   */
  static class Job9Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final Text compKey = new Text();
    private final IntWritable frequency = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;

      for (IntWritable val : values) {
        sum += Integer.parseInt(val.toString());
      }

      compKey.set(key);
      frequency.set(sum);
      context.write(compKey, frequency);
    }
  }
}
