import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

class Job8 {
  /**
   * Parses customer reviews data for asin and review text
   *
   * @param Object dummy key that can be ignored
   * @param Text   parsed product's review text
   * @return key value pair < asin, reviewText >
   */
  static class Job8Mapper extends Mapper<Object, Text, Text, Text> {
    private final static Text asinKey = new Text();
    private final static Text reviewValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      HashMap<String, Object> map = new ObjectMapper().configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
              .readValue(value.toString(), HashMap.class);

      String asin = "";
      String reviewText = "";

      if (map.containsKey("asin")) {
        asin = map.get("asin").toString();
      }
      if (map.containsKey("reviewText")) {
        reviewText = map.get("reviewText").toString().replaceAll("[^A-Za-z0-9 ]", "")
                .replaceAll("\\s+", " ");
      }

      if (!asin.equals("") && !reviewText.equals("")) {
        asinKey.set(asin);
        reviewValue.set(reviewText);
        context.write(asinKey, reviewValue);
      }
    }
  }


  /**
   * Identity reducer
   *
   * @param Text asin key
   * @param Text customer review text
   * @return < asin, { reviewText } >
   */
  static class Job8Reducer extends Reducer<Text, Text, Text, Text> {
    private final static Text reviewValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      ArrayList<String> valuesArrayList = new ArrayList<String>();
      String reviewText = "";

      for (Text val : values) {
        valuesArrayList.add(val.toString());
      }

      for (int i = 1; i < valuesArrayList.size(); i++) {
        reviewText += valuesArrayList.get(i) + " ";
      }

      if (!reviewText.isEmpty()) {
        reviewValue.set(reviewText);
        context.write(key, reviewValue);
      }
    }
  }
}
