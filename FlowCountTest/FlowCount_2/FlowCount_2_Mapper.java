package MapReduce.FlowCountTest.FlowCount_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author : HaiLiang Huang
 * @author : Always Best Sign X
 */
public class FlowCount_2_Mapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        String phoneNum = splits[0];

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(splits[1]));
        flowBean.setDownFlow(Integer.parseInt(splits[2]));
        flowBean.setUpCountFlow(Integer.parseInt(splits[3]));
        flowBean.setDownCountFlow(Integer.parseInt(splits[4]));

        context.write(flowBean, new Text(phoneNum));
    }
}
