package MapReduce.FlowCountTest.FlowCount_3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author : HaiLiang Huang
 * @author : Always Best Sign X
 */
public class FlowCount_3_Mapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        String phoneNum = splits[1];

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(splits[6]));
        flowBean.setDownFlow(Integer.parseInt(splits[7]));
        flowBean.setUpCountFlow(Integer.parseInt(splits[8]));
        flowBean.setDownCountFlow(Integer.parseInt(splits[9]));

        context.write(new Text(phoneNum), flowBean);
    }
}
