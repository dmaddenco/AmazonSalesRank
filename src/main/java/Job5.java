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
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

class Job5 {
  private static HashMap<String, String> idUniToValue = new HashMap<String, String>();

  /**
   * Map output from previous MapReduce job to new < key, value > pair and calculate SentenceTF-IDF
   * @param LongWritable object that can be ignored
   * @param Text object that contains all the output from previous MapReduce job
   * @return Key value pair < docId, {sentenceOrder \t eachSentence \t SentenceTF-IDF} >
   */
  static class Job5Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private final Text compValue = new Text();
    private final IntWritable docID = new IntWritable();

    /**
     * Read in from distributed cache and store results in memory
     * @param Context object contains file paths that were created in Driver.java
     */
    //TODO: Remove distributed cache and use instead MultipleInputs.addInputPath() and join on keys
    @Override
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

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String values[] = value.toString().split("<====>");

      //repeat Job1's document parsing and unigram creation
      if (values.length >= 3) {
        String id = values[1];
        String article = values[2];
        String[] sentences = article.split("\\.");

        for (int i = 0; i < sentences.length; i++) {
          String sentence = sentences[i];
          TreeMap<String, Double> top5Words = new TreeMap<String, Double>();
          //reversed <key, value> pair of top5Words
          TreeMap<Double, String> top5WordsTFIDF = new TreeMap<Double, String>(); //allows for reverse lookup of lowest TFIDFvalue

          if (!sentence.equals("")) {
            StringTokenizer itrWord = new StringTokenizer(sentence);

            while (itrWord.hasMoreTokens()) {
              String originalWord = itrWord.nextToken();
              String stripWord = originalWord.toLowerCase().replaceAll("[^a-z0-9 ]", "");
              String lookup = id + "\t" + stripWord;
              String comKey = id + "\t" + originalWord;

              //lookup unigram in HashMap that contains output from Job1
              if (!stripWord.equals("") && !top5Words.containsKey(comKey)) {
                try {
                  double tfidf = Double.parseDouble(idUniToValue.get(lookup).split("\t")[1]);
                  top5Words.put(comKey, tfidf);
                  top5WordsTFIDF.put(tfidf, comKey);

                  //create list of top 5 words
                  if (top5Words.size() > 5) {
                    String firstKey = top5WordsTFIDF.get(top5WordsTFIDF.firstKey());  //get lowest TFIDFvalue key's value
                    top5Words.remove(firstKey); //remove unigram w/ lowest TFIDFvalue
                    top5WordsTFIDF.remove(top5WordsTFIDF.firstKey()); //remove lowest TFIDFvalue
                  }
                } catch (NullPointerException e) {

                }
              }
            }
          }

          //calculate sentenceTFIDF from top5Words
          double sentenceTFIDF = 0;
          for (double f : top5Words.values()) {
            sentenceTFIDF += f;
          }

          //replace tabs in sentence with single space
          String tab = "\t";
          if (sentence.contains(tab)) {
            sentence = sentence.replaceAll(tab, " ");
          }

          sentence = sentence.replaceAll("\\s+", " ");  //trim extra whitespace
          String tempValue = i + "\t" + sentence + "\t" + sentenceTFIDF;  //tacking on sentence order i
          docID.set(Integer.parseInt(id));
          compValue.set(tempValue);
          context.write(docID, compValue);
        }
      }
    }
  }

  /**
   * Select top 3 sentence with highest SentenceTF-IDF
   * @param IntWritable object key that is the docId
   * @param Text object value that is the composite key of {eachSentence \t SentenceTF-IDF}
   * @return Key value pair < docId, top3Sentences >
   */
  static class Job5Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private static TreeMap<String, Double> top3 = new TreeMap<String, Double>();
    private static TreeMap<Double, String> top3TFIDF = new TreeMap<Double, String>();
    private final IntWritable docId = new IntWritable();
    private final Text top3Sentences = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      top3 = new TreeMap<String, Double>(); //used to keep sentences in order of appearance in document
      //reversed <key, value> pair of top3
      top3TFIDF = new TreeMap<Double, String>();  //used to keep TF-IDF values in order

      for (Text sentenceAndTFIDF : values) {

        if (!top3.containsKey(sentenceAndTFIDF.toString())) {
          String[] sentenceInfo = sentenceAndTFIDF.toString().split("\t");
          String sentenceOrder = sentenceInfo[0];
          String sentence = sentenceInfo[1];
          double tfidf = Double.parseDouble(sentenceInfo[2]);
          top3.put(sentenceOrder + "\t" + sentence, tfidf);
          top3TFIDF.put(tfidf, sentenceOrder + "\t" + sentence);

          if (top3.size() > 3) {
            Double lowestValue = top3TFIDF.firstKey();  //lowest TF-IDF value
            String mapKey = null;
            Set<String> keys = top3.keySet(); //composite keys that are {sentenceOrder \t sentence}

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

      //create top 3 sentences
      String tempValue = "";
      Set<String> keys = top3.keySet(); //composite keys that are {sentenceOrder \t sentence}
      for (String k : keys) {
        String[] kContent = k.split("\t");
        if (kContent.length == 2) {
          tempValue += kContent[1] + "."; //append period to sentence
        }
      }

      docId.set(Integer.parseInt(key.toString()));
      top3Sentences.set(tempValue);
      context.write(docId, top3Sentences);
    }
  }
}
