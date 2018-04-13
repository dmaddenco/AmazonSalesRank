import com.fasterxml.jackson.core.type.TypeReference;
//import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.shaded.com.fasterxml.jackson.databind.ObjectMapper;
//import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

class Job6 {
  private static HashMap<String, String> asinUniToValue = new HashMap<String, String>();
  private static HashMap<String, String> metaData = new HashMap<String, String>();
  private static HashSet<String> stopWords = new HashSet<String>();

  /**
   * Map output from previous MapReduce job of < asin, {unigram \t TFvalue \t TF-IDFvalue \t salesRank} >
   * to new < key, value > pair and calculate ProductTF-IDF by joining on key of original Customer Review data
   *
   * @param Object has dummy key
   * @param Text   contains data from the input file
   * @return Key value pair < asin, {all customer reviews \t TF-IDFvalue \t salesRank} >
   */
  static class Job6Mapper extends Mapper<Object, Text, Text, Text> {
    private final Text asinKey = new Text();
    private final Text compValue = new Text();

    @Override
    //TODO: Read in output from Job5 from MultipleInputs.addInputPath()
    //TODO: Read in metaData for product and save salesRank
    /**
     * Reads in output from Job5 and store in memory
     * @context contains file path to customer review data
     */
    /*
    public void setup(Context context) throws IOException {
      URI[] cacheFiles = context.getCacheFiles();
      if (cacheFiles != null && cacheFiles.length > 0) {
        try {
          BufferedReader reader = null;
          for (URI cacheFile1 : cacheFiles) {
            try {
              File cacheFile = new File(cacheFile1.getPath());
              reader = new BufferedReader(new FileReader(cacheFile));
              String line;
              while ((line = reader.readLine()) != null) {
                String[] lineInfo = line.split("\t");
                String docId = lineInfo[0];
                String unigram = lineInfo[1];
                String tf = lineInfo[2];
                String tfid = lineInfo[3];

                String key = docId + "\t" + unigram;
                String value = tf + "\t" + tfid;

                idUniToValue.put(key, value);
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
    */

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      HashMap<String, Object> map = new ObjectMapper().readValue(value.toString(), HashMap.class);
      String asin = map.get("asin").toString();
      String reviewText = map.get("reviewText").toString();
      String salesRank = "";

      if (reviewText != null && reviewText.length() != 0) {
        String[] sentences = reviewText.split("\\.");
        for (String sentence : sentences) {
          if (!sentence.equals("")) {
            StringTokenizer itrWord = new StringTokenizer(sentence);
            TreeMap<String, Double> top5Words = new TreeMap<String, Double>();
            TreeMap<Double, String> top5WordsTFIDF = new TreeMap<Double, String>();

            while (itrWord.hasMoreTokens() && !stopWords.contains(itrWord.toString())) {
              String unigram = itrWord.nextToken().toLowerCase().replaceAll("[^a-z0-9 ]", "");
              String mapKey = asin + "\t" + unigram;
              if (!top5Words.containsKey(mapKey) && asinUniToValue.containsKey(mapKey)) {
                double tfidf = Double.parseDouble(asinUniToValue.get(mapKey).split("\t")[1]);
                top5Words.put(mapKey, tfidf);
                top5WordsTFIDF.put(tfidf, mapKey);

                if (top5Words.size() > 5) {
                  String firstKey = top5WordsTFIDF.get(top5WordsTFIDF.firstKey());
                  top5Words.remove(firstKey);
                  top5WordsTFIDF.remove(top5WordsTFIDF.firstKey());
                }
              }
            }

            double productTFIDF = 0;
            for (double f : top5Words.values()) {
              productTFIDF += f;
            }

            asinKey.set(asin);
            //TODO: get salesRank
            compValue.set(productTFIDF + "\t" + salesRank);
            context.write(asinKey, compValue);
          }
        }
      }
    }
  }

  static class Job6Reducer extends Reducer<Text, Text, Text, Text> {
    private static TreeMap<String, Double> top3 = new TreeMap<String, Double>();
    private static TreeMap<Double, String> top3TFIDF = new TreeMap<Double, String>();
    private final Text docId = new Text();
    private final Text top3Sentences = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
      top3 = new TreeMap<String, Double>();
      top3TFIDF = new TreeMap<Double, String>();

      for (Text sentenceAndTFIDF : values) {

        if (!top3.containsKey(sentenceAndTFIDF.toString())) {
          String[] sentenceInfo = sentenceAndTFIDF.toString().split("\t");
          String sentenceOrder = sentenceInfo[0];
          String sentence = sentenceInfo[1];
          double tfidf = Double.parseDouble(sentenceInfo[2]);
          top3.put(sentenceOrder + "\t" + sentence, tfidf);
          top3TFIDF.put(tfidf, sentenceOrder + "\t" + sentence);

          if (top3.size() > 3) {
            Double lowestValue = top3TFIDF.firstKey();
            String mapKey = null;
            Set<String> keys = top3.keySet();

            for (String k : keys) {
              if (top3.get(k).equals(lowestValue)) {
                top3TFIDF.remove(lowestValue);
                mapKey = k;
                break;
              }
            }

            top3.remove(mapKey);
          }
        }
      }

      String tempValue = "";
      Set<String> keys = top3.keySet();
      for (String k : keys) {
        String[] kContent = k.split("\t");
        if (kContent.length == 2) {
          tempValue += kContent[1] + ".";
        }
      }

      docId.set(key.toString());
      top3Sentences.set(tempValue);
      context.write(docId, top3Sentences);
    }
  }
}