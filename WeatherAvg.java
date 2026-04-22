import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAvg {

    // Mapper Class
    public static class AvgMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            // Skip empty lines
            if (line.isEmpty()) return;

            String[] fields = line.split(",");

            // Expected format: date,temp,dew,wind
            if (fields.length == 4) {
                try {
                    double temp = Double.parseDouble(fields[1]);
                    double dew = Double.parseDouble(fields[2]);
                    double wind = Double.parseDouble(fields[3]);

                    // Emit a constant key so all data goes to one reducer
                    context.write(new Text("avg"),
                            new Text(temp + "," + dew + "," + wind + ",1"));

                } catch (NumberFormatException e) {
                    // Skip bad records
                }
            }
        }
    }

    // Reducer Class
    public static class AvgReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double tempSum = 0;
            double dewSum = 0;
            double windSum = 0;
            int count = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");

                tempSum += Double.parseDouble(parts[0]);
                dewSum += Double.parseDouble(parts[1]);
                windSum += Double.parseDouble(parts[2]);
                count += Integer.parseInt(parts[3]);
            }

            double avgTemp = tempSum / count;
            double avgDew = dewSum / count;
            double avgWind = windSum / count;

            String result = "Avg Temp = " + avgTemp +
                            ", Avg Dew = " + avgDew +
                            ", Avg Wind = " + avgWind;

            context.write(new Text("Result"), new Text(result));
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: WeatherAvg <input file> <output folder>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Average");

        job.setJarByClass(WeatherAvg.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
