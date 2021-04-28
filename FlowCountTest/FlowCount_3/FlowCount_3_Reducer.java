package MapReduce.FlowCountTest.FlowCount_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : HaiLiang Huang
 * @author : Always Best Sign X
 */
public class FlowCount_3_Reducer extends Reducer<Text, FlowBean,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        MapReduce.FlowCountTest.FlowCount_1.FlowBean flowBean = new MapReduce.FlowCountTest.FlowCount_1.FlowBean();
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upCountFlow = 0;
        Integer downCountFlow = 0;
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);

        context.write(key, new Text(flowBean.toString()));
    }
}
