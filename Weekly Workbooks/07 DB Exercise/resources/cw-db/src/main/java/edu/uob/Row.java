package edu.uob;
import java.util.ArrayList;

public class Row {
    private int id;
    private ArrayList<String> values;

    public Row(int id){
        this.id = id;
        this.values = new ArrayList<>();
    }

    public int getId() {
        return id;
    }

    public ArrayList<String> getValues() {
        return values;
    }

    public void addValue(String value){
        values.add(value);
    }

    @Override
    public String toString() {
        // This helps when writing back to the .tab file later
        return id + "\t" + String.join("\t", values);
    }
}
