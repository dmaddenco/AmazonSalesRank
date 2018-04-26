import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

//import org.json.simple.*;
//import com.fasterxml.jackson.databind.ObjectMapper;

class Job1 {
  /**
   * Parses metadata for asin and sales rank
   *
   * @param asin parsed product asin number
   * @param rank parsed product sales rank number
   * @return (asin, rank)
   */
  public static class Job0Mapper extends Mapper<Object, Text, Text, Text> {
    private final static Text asinKey = new Text();
    private final static Text rank = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      HashMap<String, Object> map = new ObjectMapper().configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
              .configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true)
              .readValue(value.toString(), HashMap.class);

      String asin = "";
      String salesRank = "";

      if (map.containsKey("asin")) {
        asin = map.get("asin").toString();
      }
      if (map.containsKey("salesRank")) {
        String temp = map.get("salesRank").toString();
        if (temp.contains("Electronics")) {
          JSONObject nestedKey;
          try {
            nestedKey = new JSONObject(temp);
            if (nestedKey.has("Electronics")) {
              salesRank = nestedKey.get("Electronics").toString();
            }
          } catch (JSONException e) {
            e.printStackTrace();
          }
        }
      }

      if (!asin.equals("") && !salesRank.equals("")) {
        asinKey.set(asin);
        rank.set(salesRank);
        context.write(asinKey, rank);
      }
    }
  }

  /**
   * Parses customer reviews data for asin and review text
   *
   * @param asin parsed product asin number
   * @param data parsed product's review text
   * @return (asin, reviewText)
   */
  static class Job1Mapper extends Mapper<Object, Text, Text, Text> {
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
   * Joins customer reviews and sales rank on asin key
   *
   * @param result value of review text and rank stored as text object
   * @param combo  value of review text and rank stored as string object
   * @return (asin, { reviewText, rank })
   */
  static class Job1Reducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();
    private String combo = "";

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      ArrayList<String> valuesArrayList = new ArrayList<String>();
      String rank = "";
      String reviewText = "";

      for (Text val : values) {
        valuesArrayList.add(val.toString());
      }

      if (valuesArrayList.size() >= 2) {
        try {
          int rankTest = Integer.parseInt(valuesArrayList.get(0));
          rank = valuesArrayList.get(0);
          if (valuesArrayList.size() > 2) {
            for (int i = 1; i < valuesArrayList.size(); i++) {
              reviewText += valuesArrayList.get(i) + " ";
            }
          } else {
            reviewText = valuesArrayList.get(1);
          }
        } catch (Exception e) {
          rank = "";
          reviewText = "";
        }
      }

      if (!reviewText.isEmpty() && !rank.isEmpty()) {
        combo = reviewText + "\t" + rank;
        result.set(combo);
        context.write(key, result);
      }
    }
  }
}
