import model.AbstractCustomerEntity;
import model.Customer;
import model.CustomerOrdersVO;
import model.Orders;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Objects;

public class JoinReducerTaskII extends Reducer<IntWritable, GenericCustomerEntity, NullWritable, Text> {

    @Override
    protected void reduce(IntWritable key,
                          Iterable<GenericCustomerEntity> values,
                          Reducer<IntWritable, GenericCustomerEntity, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        Customer customer = null;
        Orders orders = null;

        for (GenericCustomerEntity genericObject : values) {
            AbstractCustomerEntity f = (AbstractCustomerEntity) genericObject.get();
            if (f instanceof Customer) {
                customer = (Customer) f;
            }
            else if (f instanceof Orders) {
                orders = (Orders) f;
                }
            }
        if (Objects.nonNull(customer) && !Objects.nonNull(orders)) {
            context.write(NullWritable.get(), new Text(customer.getName()));
        }
    }
}