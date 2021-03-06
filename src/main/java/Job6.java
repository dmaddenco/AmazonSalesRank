import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

class Job6 {
  /**
   * Map output from previous MapReduce job of < asin, {unigram \t TFvalue \t TF-IDFvalue \t salesRank} >
   * to new < key, value > pair < salesRank, RangeTF-IDFValue >
   *
   * @param LongWritable object that can be ignored
   * @param Text         contains output from previous job < asin, {unigram \t TFvalue \t TF-IDFvalue \t salesRank} >
   */
  static class Job6Mapper extends Mapper<Object, Text, Text, Text> {
    private final Text salesRankKey = new Text();
    private final Text tfidfValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] values = value.toString().split("\t");

      if (values.length == 5) {
//        String asin = values[0];
//        String unigram = values[1];
//        String tfValue = values[2];
        String tfidf = values[3];
        String salesRank = values[4];
        salesRankKey.set(salesRank);
        tfidfValue.set(tfidf);
        context.write(salesRankKey, tfidfValue);
      }
    }
  }

  /**
   * Calculate RangeTF-IDFValue
   *
   * @param Text contains {RangeTF-IDFValue, salesRank}
   * @param Text contains {RangeTF-IDFValue, salesRank}
   */
  static class Job6Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text salesRankKey = new Text();
    private final Text rangeTFIDFValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      TreeMap<Double, String> productTFIDF = new TreeMap<Double, String>();
      salesRankKey.set(key);
      double rangeTFIDFSum = 0;

      for (Text v : values) {
        double tfIDF = Double.parseDouble(v.toString());
        if (!productTFIDF.containsKey(tfIDF)) {
          productTFIDF.put(tfIDF, key.toString());
          if (productTFIDF.size() > 10) {
            productTFIDF.remove(productTFIDF.firstKey());
          }
        }
      }

      for (Map.Entry<Double, String> entry : productTFIDF.entrySet()) {
        double tfIDF = entry.getKey();
        rangeTFIDFSum += tfIDF;
      }

      rangeTFIDFValue.set(Double.toString(rangeTFIDFSum));
      context.write(salesRankKey, rangeTFIDFValue);
    }
  }
}