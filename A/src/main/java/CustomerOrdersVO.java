import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomerOrdersVO extends AbstractCustomerEntity{
    private String name;
    private String address;
    private Double priceAvg;

    public CustomerOrdersVO(int id, String name, String address, Double priceAvg) {
        super(id);
        this.name = name;
        this.address = address;
        this.priceAvg = priceAvg;
    }

    public CustomerOrdersVO() {

    }

    @Override
    public int compareTo(AbstractCustomerEntity o) {
        return Integer.compare(this.id, o.getId());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(address);
        dataOutput.writeDouble(priceAvg);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        name = dataInput.readUTF();
        address = dataInput.readUTF();
        priceAvg = dataInput.readDouble();
    }

    @Override
    public String toString(){
        return name + "," + address +"," +priceAvg;
    }
}
