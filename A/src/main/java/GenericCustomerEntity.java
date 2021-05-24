import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("unchecked")
public class GenericCustomerEntity extends GenericWritable {
    private static Class<? extends Writable>[] CLASSES;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[]{
                Customer.class,
                Orders.class
                //add as many different class as you want
        };
    }


    public GenericCustomerEntity(Writable writable) {
        set(writable);
    }

    public GenericCustomerEntity(){

    }
    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }
}
