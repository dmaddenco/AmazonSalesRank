import com.fasterxml.jackson.core.JsonParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.json.simple.*;
//import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.HashMap;
import java.io.IOException;
import java.util.StringTokenizer;

class Job1 {
  /**
   * Parses metadata for asin and sales rank
   *
   * @param asin parsed product asin number
   * @param rank parsed product sales rank number
   * @return (asin, rank)
   */
  public static class Job0Mapper extends Mapper<Object, Text, Text, Text> {
    //private final static DocComposite doc = new DocComposite();
//    private final static Text asin = new Text();
    private final static Text asinKey = new Text();
    private final static Text rank = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//      String meta = value.toString();
//      if (!meta.isEmpty()) {
//        if (meta.contains("asin") && meta.contains("'salesRank': {'Electronics':")) {
//
//          StringTokenizer itrWord = new StringTokenizer(meta);
//
//          while (itrWord.hasMoreTokens()) {
//            String unigram = itrWord.nextToken().toLowerCase().replaceAll("[^A-Za-z0-9]", "");
//            if (unigram.equals("asin")) {
//              String num = itrWord.nextToken().toLowerCase().replaceAll("[^A-Za-z0-9]", "");
//              asin.set(num);
//            }
//            if (unigram.equals("salesrank")) {
//              String temp = itrWord.nextToken();
//              String electronicText = itrWord.nextToken().toLowerCase().replaceAll("[^A-Za-z0-9]", "");
//              rank.set(electronicText);
//            }
//          }
//          context.write(asin, rank);
//        }
//      }
      HashMap<String, Object> map = new ObjectMapper().configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true).readValue(value.toString(), HashMap.class);
      String asin = "";
      String salesRank = "";

      if (map.containsKey("asin")) {
        asin = map.get("asin").toString();
      }
      if (map.containsKey("salesRank")) {
        String temp = map.get("salesRank").toString();
        JSONObject nestedKey;
        try {
          nestedKey = new JSONObject(temp);
          salesRank = nestedKey.get("Electronics").toString();
        } catch (JSONException e) {
          e.printStackTrace();
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
    private final static Text asin = new Text();
    private final static Text data = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String reviews = value.toString();
      if (!reviews.isEmpty()) {
        HashMap<String, Object> map = new ObjectMapper().readValue(reviews, HashMap.class);
        String tempAsin = map.get("asin").toString();
        String reviewText = map.get("reviewText").toString();

        asin.set(tempAsin);
        data.set(reviewText);
        context.write(asin, data);

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

      String rank = "";
      String reviewText = "";
      for (Text val : values) {
        String temp = val.toString();
        char myChar = temp.charAt(0);

        if (Character.isDigit(myChar)) {
          rank = temp;
        } else {
          reviewText = temp;
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
