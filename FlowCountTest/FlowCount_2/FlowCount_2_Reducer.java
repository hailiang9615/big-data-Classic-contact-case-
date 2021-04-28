package MapReduce.FlowCountTest.FlowCount_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : HaiLiang Huang
 * @author : Always Best Sign X
 */
public class FlowCount_2_Reducer extends Reducer<FlowBean, Text,Text,Text> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        for (Text value : values) {
            context.write(new Text(value),new Text(key.toString()));
        }
    }
}
