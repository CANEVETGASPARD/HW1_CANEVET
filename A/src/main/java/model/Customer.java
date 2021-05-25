package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Customer extends AbstractCustomerEntity {
    private String name;
    private String address;
    private String nationkey;
    private String phone;
    private String acctbal;
    private String mktsegment;
    private String comment;

    public Customer(int id, String name, String address, String nationkey, String phone,String acctbal, String mktsegment, String comment){
        super(id);
        this.name = name;
        this.address = address;
        this.nationkey = nationkey;
        this.phone = phone;
        this.acctbal = acctbal;
        this.mktsegment = mktsegment;
        this.comment = comment;
    }

    public Customer() {

    }

    public int compareTo(AbstractCustomerEntity o) {
        return Integer.compare(this.id, o.getId());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(address);
        dataOutput.writeUTF(nationkey);
        dataOutput.writeUTF(phone);
        dataOutput.writeUTF(acctbal);
        dataOutput.writeUTF(mktsegment);
        dataOutput.writeUTF(comment);

    }

    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        name = dataInput.readUTF();
        address = dataInput.readUTF();
        nationkey = dataInput.readUTF();
        phone = dataInput.readUTF();
        acctbal = dataInput.readUTF();
        mktsegment = dataInput.readUTF();
        comment = dataInput.readUTF();
    }

    public String getActball(){
        return this.acctbal;
    }

    public String getName(){
        return this.name;
    }

    public String getAddress() {
        return this.address;
    }
}
