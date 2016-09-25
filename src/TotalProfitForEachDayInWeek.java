/**
 * Created by goran on 9/25/16.
 * Processing the data from http://content.udacity-data.com/courses/ud617/purchases.txt.gz
 * and printing the profit made each day in the week. So the resulting file contains 7 rows.
 * In each row is printed one of the days in the week and the total profit for that day (during one year time).
 */
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class TotalProfitForEachDayInWeek {
    public static final String[] days = {"", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text dayKey = new Text();
        private DoubleWritable amountValue = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, DoubleWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            if(line == null || line.trim().length() == 0) {
                return;
            }

            try {
                String[] spl = line.split("\t");
                Calendar calendar = Calendar.getInstance();
                Date date = new SimpleDateFormat("yyyy-MM-dd").parse(spl[0]);
                System.out.println(date.toString());
                calendar.setTime(date);
                int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);

                double amount = Double.parseDouble(spl[4]);

                String day = Project2.days[dayOfWeek];

                dayKey.set(day);
                amountValue.set(amount);
                output.collect(dayKey, amountValue);
            }
            catch(ArrayIndexOutOfBoundsException ex) {
                System.out.println("Parse error for " + line);
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

    }

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterator<DoubleWritable> values,
                           OutputCollector<Text, DoubleWritable> output, Reporter reporter)
                throws IOException {
            double total = 0.0;
            while (values.hasNext()) {
                total += values.next().get();
            }
            output.collect(key, new DoubleWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Project2.class);
        conf.setJobName("project2");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);
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