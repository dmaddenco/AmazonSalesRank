import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

class Job13 {
  /**
   * Map output from previous MapReduce job of < asin, {unigram \t TFvalue \t TF-IDFvalue} >
   * to new < key, value > pair < asin, RangeTF-IDFValue >
   *
   * @param LongWritable object that can be ignored
   * @param Text         contains output from previous job < asin, {unigram \t TFvalue \t TF-IDFvalue } >
   */
  static class Job13Mapper extends Mapper<Object, Text, Text, Text> {
    private final Text asinKey = new Text();
    private final Text tfidfValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] values = value.toString().split("\t");

      if (values.length == 4) {
        String asin = values[0];
//        String unigram = values[1];
//        String tfValue = values[2];
        String tfidf = values[3];

        asinKey.set(asin);
        tfidfValue.set(tfidf);
        context.write(asinKey, tfidfValue);
      }
    }
  }

  /**
   * Calculate RangeTF-IDFValue
   *
   * @param Text contains asin
   * @param Text contains ProductTF-IDFvalue
   */
  static class Job13Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text asinKey = new Text();
    private final Text rangeTFIDFValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      TreeMap<Double, String> productTFIDF = new TreeMap<Double, String>();
      asinKey.set(key);
      double rangeTFIDFSum = 0;

      for (Text v : values) {
        double tfIDF = Double.parseDouble(v.toString());
        if (!productTFIDF.containsKey(tfIDF)) {
          productTFIDF.put(tfIDF, key.toString());
        }
      }

      int i = 0;
      for (Map.Entry<Double, String> entry : productTFIDF.entrySet()) {
        if (i < 10) {
          Double tfIDF = entry.getKey();
//          String value = entry.getValue();
          rangeTFIDFSum += tfIDF;
        }
        i++;
      }

      rangeTFIDFValue.set(Double.toString(rangeTFIDFSum));
      context.write(asinKey, rangeTFIDFValue);
    }
  }
}