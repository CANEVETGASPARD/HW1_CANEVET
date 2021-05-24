import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Orders extends AbstractCustomerEntity {
    private String custkey;
    private String orderstatus;
    private String price;
    private String orderdate;
    private String orderpriority;
    private String clerk;
    private String shippriority;
    private String comment;

    public Orders(int id, String custkey, String orderstatus, String price, String orderdate, String orderpriority, String clerk, String shippriority, String comment) {
        super(id);
        this.custkey = custkey;
        this.orderstatus = orderstatus;
        this.price = price;
        this.orderdate = orderdate;
        this.orderpriority = orderpriority;
        this.clerk = clerk;
        this.shippriority = shippriority;
        this.comment = comment;
    }

    public Orders() {

    }

    @Override
    public int compareTo(AbstractCustomerEntity o) {
        return Integer.compare(this.id, o.getId());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(custkey);
        dataOutput.writeUTF(orderstatus);
        dataOutput.writeUTF(price);
        dataOutput.writeUTF(orderdate);
        dataOutput.writeUTF(orderpriority);
        dataOutput.writeUTF(clerk);
        dataOutput.writeUTF(shippriority);
        dataOutput.writeUTF(comment);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        custkey = dataInput.readUTF();
        orderstatus = dataInput.readUTF();
        price = dataInput.readUTF();
        orderdate = dataInput.readUTF();
        orderpriority = dataInput.readUTF();
        clerk = dataInput.readUTF();
        shippriority = dataInput.readUTF();
        comment = dataInput.readUTF();

    }

    public String getOrderDate() {
        return this.orderdate;
    }

    public String getPrice() {
        return this.price;
    }

    public String toString() {
        return id + "," + custkey + "," + orderstatus + "," + price + "," + orderdate + "," + orderpriority + "," + clerk + "," + shippriority + "," + comment;
    }
}
