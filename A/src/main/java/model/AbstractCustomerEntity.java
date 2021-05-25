package model;

import org.apache.hadoop.io.WritableComparable;

public abstract class AbstractCustomerEntity implements WritableComparable<AbstractCustomerEntity>{
    protected int id;

    public AbstractCustomerEntity(int id){
        this.id = id;
    }

    public AbstractCustomerEntity() {

    }

    public int getId() {
        return this.id;
    }
}
