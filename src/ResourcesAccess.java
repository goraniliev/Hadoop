/**
 * Created by goran on 9/25/16.
 * Processes the logs from http://content.udacity-data.com/courses/ud617/access_log.gz
 * Prints out the ip addresses from which each of the files is accessed in each of the 12 months, each line is in the format:
 * month file ip_address_1 ip_address_2 ...
 */
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ResourcesAccess {
    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {
        private Text monthFileKey = new Text();
        private Text ipValue = new Text();

        @Override
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            if(line == null || line.trim().length() == 0) {
                return;
            }
            String[] spl = line.split("\\s+");

            try {
                String ip = spl[0];
                String[] timeSpl = spl[3].split("/");
                String month = timeSpl[1];
                String fileName = spl[6];
                System.out.println(ip + " " + month + " " + fileName);

                monthFileKey.set(month.trim() + " " + fileName.trim());
                ipValue.set(ip);
                output.collect(monthFileKey, ipValue);
            }
            catch(ArrayIndexOutOfBoundsException ex) {
                System.out.println("Parse error for " + line);
            }

        }

    }

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            StringBuffer sb = new StringBuffer();
            while (values.hasNext()) {
                sb.append(values.next().toString() + " ");
            }
            output.collect(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Lab5.class);
        conf.setJobName("lab5");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
