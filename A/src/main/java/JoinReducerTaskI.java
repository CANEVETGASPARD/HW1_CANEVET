import model.AbstractCustomerEntity;
import model.Customer;
import model.CustomerOrdersVO;
import model.Orders;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Objects;

public class JoinReducerTaskI extends Reducer<IntWritable, GenericCustomerEntity, NullWritable, CustomerOrdersVO> {

    @Override
    protected void reduce(IntWritable key,
                          Iterable<GenericCustomerEntity> values,
                          Reducer<IntWritable, GenericCustomerEntity, NullWritable, CustomerOrdersVO>.Context context) throws IOException, InterruptedException {
        double num = 0;
        double denom = 0;
        Customer customer = null;
        Orders orders = null;

        for (GenericCustomerEntity genericObject : values) {
            AbstractCustomerEntity f = (AbstractCustomerEntity) genericObject.get();
            if (f instanceof Customer) {
                customer = (Customer) f;
            }
            else if (f instanceof Orders) {
                orders = (Orders) f;
                LocalDate ordersDate = LocalDate.parse(orders.getOrderDate());
                if(ordersDate.isAfter(LocalDate.parse("1996-01-01"))){
                    System.out.println(key);
                    num += Double.parseDouble(orders.getPrice());
                    denom ++;
                }
            }
        }
        if (Objects.nonNull(customer) && denom!=0 && Double.parseDouble(customer.getActball()) > 2000) {
            double average = num/denom;
            context.write(NullWritable.get(), merge(customer, average));
        }
    }

    private CustomerOrdersVO merge(Customer customer, Double priceAvg) {
        return new CustomerOrdersVO(customer.getId(),customer.getName(),customer.getAddress(),priceAvg);
    }

}
