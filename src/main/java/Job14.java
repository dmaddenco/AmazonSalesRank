import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;

class Job14 {

  private static TreeMap<Double, String> rangeBins = new TreeMap<Double, String>();

  static class Job14Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text asin = new Text();
    private final Text tfidfRank = new Text();

    private long numReduceTasks;

    /**
     * Get number of reducers from context and set equal to local variable
     *
     * @param context contains count value from driver
     */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
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
                String[] part = line.split("\t");
                double maxTFIDF = Double.parseDouble(part[1]);
                rangeBins.put(maxTFIDF, part[3] + "\t" + part[0] + "\t" + part[2]);
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

      String[] values = value.toString().split("\t");
      String asinNum = values[0];
      double prodTFIDF = Double.parseDouble(values[1]);

      //rangeBins.put(prodTFIDF,asinNum);
      double maxKey;
      try {
        maxKey = rangeBins.higherKey(prodTFIDF);
      } catch (Exception e) {
        maxKey = rangeBins.lastKey();
      }
      String valRanges = rangeBins.get(maxKey);
      String[] part = valRanges.split("\t");

      asin.set(asinNum);
      tfidfRank.set("TFIDF: " + values[1] + "\tSALES RANK RANGE: " + part[2] + " - " + part[1]);

      context.write(asin, tfidfRank);
    }
  }

  static class Job14Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text maxValues = new Text();
    private final Text minValues = new Text();


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      for (Text val : values) {
        context.write(key, val);
      }

    }
  }
}
