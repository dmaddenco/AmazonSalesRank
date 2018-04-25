import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.lang.*;

class Job7{

    /**
   * Identity mapper
   *
   * @param LongWritable object that can be ignored
   * @param Text         object that contains all the output from previous MapReduce job
   * @param salesRank    the sale's rank of the product
   * @param TFIDF        the range-TFIDF value
   * @return Key value pair < salesRank, TFIDF >
   */
    static class Job7Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text salesRank = new Text();
        private final Text TFIDF = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
            String[] values = value.toString().split("\t");
            String rank = values[0];
            String tfidf = values[1];
            
            salesRank.set(rank);
            TFIDF.set(tfidf);
            context.write(salesRank,TFIDF);
        }
    }
    
    static class Job7Reducer extends Reducer<Text, Text, Text, Text> {
        private final Text maxValues = new Text();
        private final Text minValues = new Text();
        
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            Long maxRank = 0;
            Long minRank = Long.MAX_VALUE;
            Double maxtfidf = 0;   
            Double mintfidf = Double.MAX_VALUE;
            
            for (Text val : values) {
                String [] part = val.toString().split("\t");
                Long tempRank = Long.parseLong(part[0]);
                Double tempTFIDF = Double.parseDouble(part[1]);
                
                if(tempRank > maxRank){
                    maxRank = tempRank;
                }
                if(tempRank < minRank){
                    minRank = tempRank;
                }
                if(tempTFIDF > maxtfidf){
                    maxtfidf = tempTFIDF;
                }
                if(tempTFIDF < mintfidf){
                    mintfidf = tempTFIDF
                }
            }
            
            maxValues.set(Long.toString(maxRank) + "\t" + Double.toString(maxtfidf));
            minValues.set(Long.toString(minRank) + "\t" + Double.toString(mintfidf));
            context.write(maxValues,minValues);
        }
    }
}
