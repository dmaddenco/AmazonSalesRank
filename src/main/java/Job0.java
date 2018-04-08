import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput; 
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.MapWritable;
import java.util.ArrayList;
import java.util.HashSet;
import java.lang.Math;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import java.util.Collections;
import java.util.TreeMap;
import java.util.HashMap;
import java.net.URI;
import java.io.*;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import java.util.regex.Pattern;

class Job0{

    public static class Job0Mapper extends Mapper<Object, Text, Text, Text>{
        //private final static DocComposite doc = new DocComposite();
        private final static Text asin = new Text();
        private final static Text rank = new Text();
        private final static Text word = new Text("test");
        private final static Text word2 = new Text("value");
        //private final static IntWritable one = new IntWritable(1);
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String meta = value.toString();
            if (!meta.isEmpty()) {
                if(meta.contains("asin") && meta.contains("'salesRank': {'Electronics':")){
                    /*int asinBegin = meta.indexOf("asin") + 8;
                    int asinEnd = asinBegin + 10;
                    String asinString = meta.substring(asinBegin,asinEnd);
                    asin.set(asinString);*/
                    
                    StringTokenizer itrWord = new StringTokenizer(meta);
                    //StringTokenizer itrWord = new StringTokenizer(meta.replaceAll("[^A-Za-z0-9]",""));
                    //String salesRank = "";
                    //String tempAsin = "";
                    while (itrWord.hasMoreTokens()) {
                        String unigram = itrWord.nextToken().toLowerCase().replaceAll("[^A-Za-z0-9]","");
                        if(unigram.equals("asin")){
                        //context.write(word,word2);
                            String num = itrWord.nextToken().toLowerCase().replaceAll("[^A-Za-z0-9]","");
                            //String num = itrWord.nextToken().toLowerCase().replaceAll("[^A-Za-z0-9]","");
                            asin.set(num);
                        }
                        if(unigram.equals("salesrank")){
                            String temp = itrWord.nextToken();
                            String electronicText = itrWord.nextToken().toLowerCase().replaceAll("[^A-Za-z0-9]","");
                            //String num = itrWord.nextToken().toLowerCase().replaceAll("[^A-Za-z0-9]","");
                            rank.set(electronicText);
                            //rank.set("GOT");
                        }
                        //break;
                    }
                    //asin.set(tempAsin);
                    //rank.set(salesRank);
                    context.write(asin,rank);
                }
            }
        }
    }
    
        
        public static class Job0Reducer extends Reducer<Text,Text,Text,Text> {
        
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key,val);
            }
        }
    }
}
