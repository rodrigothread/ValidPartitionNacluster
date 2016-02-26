/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package validpartitionnacluster;

import com.sun.jmx.snmp.BerDecoder;
import java.io.IOException;
import java.net.InetAddress;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author rodrigob
 */
public class ValidPartitionNacluster extends Configured implements Tool {

   //mapper output format : gender is the key, the value is formed by concatenating the name, age and the score
    // the type parameters are the input keys type, the input values type, the
    // output keys type, the output values type
    Configuration conf = new Configuration();

    public static class PartitionMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            
            String[] tokens = value.toString().split(";");
             StringTokenizer itr = new StringTokenizer(tokens[0]);
        
             while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }

            //String nameAgeScore = tokens[0] + " " + tokens[1] + " " + tokens[3];
            //the mapper emits key, value pair where the key is the gender and the value is the other information which includes name, age and score
          
        }
    }

    //AgePartitioner is a custom Partitioner to partition the data according to age.
    //The age is a part of the value from the input file.
    //The data is partitioned based on the range of the age.
    //In this example, there are 3 partitions, the first partition contains the information where the age is less than 20
    //The second partition contains data with age ranging between 20 and 50 and the third partition contains data where the age is >50.
    public static class AgePartitioner extends Partitioner<Text, IntWritable> {

        public int getPartition(Text key, IntWritable value, int numReduceTasks) {

            String partition = key.toString();
            System.err.print(partition+"\t");
           // String localidade = partition[0];
            //int ageInt = Integer.parseInt(localidade);

            //this is done to avoid performing mod with 0
            if (partition == "p_0") {
             return 0;
             }else
             //if the age is <20, assign partition 0
             if (partition.equals("p_1")) {
             return 1 % numReduceTasks;
             }else
             if (partition.equals("p_2")) {
             return 2 % numReduceTasks;
             }else
             if (partition.equals("p_3")) {
             return 3 % numReduceTasks;
             }else
             
            return 0;
            

        }
    }

    //The data belonging to the same partition go to the same reducer. In a particular partition, all the values with the same key are iterated and the person with the maximum score is found.
    //Therefore the output of the reducer will contain the male and female maximum scorers in each of the 3 age categories.
    // the type parameters are the input keys type, the input values type, the
    // output keys type, the output values type
    static class ParitionReducer extends Reducer<Text, IntWritable, Text, Text> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
                
                 InetAddress ip;
            ip = InetAddress.getLocalHost();
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            Text myHost =new Text();
           myHost.set(ip.getHostAddress()+" " + Integer.toString(sum));
            //result.set(sum);
            context.write(key , myHost);

        }
    }

    // the driver to execute two jobs and invoke the map/reduce functions
    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();

        Job job = Job.getInstance(getConf());

        job.setJarByClass(ValidPartitionNacluster.class); //é necessário(corrigido)

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(PartitionMapper.class);
        job.setPartitionerClass(AgePartitioner.class);
        job.setReducerClass(ParitionReducer.class);
       
        //Number of Reducer tasks.
        job.setNumReduceTasks(4);
        
        job.setJobName("Partition for Machine Count");
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, new Path(args[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new ValidPartitionNacluster(), args);

    }
}
