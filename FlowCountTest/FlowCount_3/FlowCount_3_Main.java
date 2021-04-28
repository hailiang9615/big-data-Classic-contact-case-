package MapReduce.FlowCountTest.FlowCount_3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author : HaiLiang Huang
 * @author : Always Best Sign X
 */
public class FlowCount_3_Main extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "FlowCount_1");
        job.setJarByClass(FlowCount_3_Main.class);


        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("D:\\input\\FlowCount"));

        job.setMapperClass(FlowCount_3_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setPartitionerClass(FlowPartition.class);
        job.setNumReduceTasks(4);

        job.setReducerClass(FlowCount_3_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("D:\\output_out\\FlowCount444"));
        boolean bl = job.waitForCompletion(true);
        return bl ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new FlowCount_3_Main(), args);
        System.exit(run);
    }
}