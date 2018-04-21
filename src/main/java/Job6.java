import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
            TreeMap<String, Double> top10Words = new TreeMap<String, Double>();
            TreeMap<Double, String> top10WordsTFIDF = new TreeMap<Double, String>();

            while (itrWord.hasMoreTokens() && !stopWords.contains(itrWord.toString())) {
              String unigram = itrWord.nextToken().toLowerCase().replaceAll("[^a-z0-9 ]", "");
              String mapKey = asin + "\t" + unigram;
              if (!top10Words.containsKey(mapKey) && asinUniToValue.containsKey(mapKey)) {
                double tfidf = Double.parseDouble(asinUniToValue.get(mapKey).split("\t")[1]);
                top10Words.put(mapKey, tfidf);
                top10WordsTFIDF.put(tfidf, mapKey);

                if (top10Words.size() > 10) {
                  String firstKey = top10WordsTFIDF.get(top10WordsTFIDF.firstKey());
                  top10Words.remove(firstKey);
                  top10WordsTFIDF.remove(top10WordsTFIDF.firstKey());
                }
              }
            }

            double productTFIDF = 0;
            for (double f : top10Words.values()) {
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
    private final Text asin = new Text();
    private final Text comKey = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

      asin.set(key);
      for (Text v : values) {
        comKey.set(v);
        context.write(asin, comKey);
      }
    }
  }
}