import model.AbstractCustomerEntity;
import model.Customer;
import model.Orders;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, IntWritable, GenericCustomerEntity>{

    @Override
    protected void map(LongWritable key,
                       Text value,
                       Mapper<LongWritable, Text, IntWritable, GenericCustomerEntity>.Context context) throws IOException, InterruptedException {

        String[] tuple = value.toString().split("\\|");
        AbstractCustomerEntity v;

        if (tuple.length >= 9) {
            v = new Orders(tuple[0],
                    Integer.parseInt(tuple[1]),
                    tuple[2],
                    tuple[3],
                    tuple[4],
                    tuple[5],
                    tuple[6],
                    tuple[7],
                    tuple[8]);
            context.write(new IntWritable(Integer.parseInt(tuple[1])), new GenericCustomerEntity(v));
        } else {
            v = new Customer(Integer.parseInt(tuple[0]),
                    tuple[1],
                    tuple[2],
                    tuple[3],
                    tuple[4],
                    tuple[5],
                    tuple[6],
                    tuple[7]);
            context.write(new IntWritable(Integer.parseInt(tuple[0])), new GenericCustomerEntity(v));
        }

    }

}
