package tn.insat.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Ligne attendue (colonnes separees par des tabulations) :
 * date  temps  magasin  produit  cout  paiement
 */
public class SalesMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text store = new Text();
    private DoubleWritable cost = new DoubleWritable();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length < 5) {
            return;
        }
        store.set(fields[2]);
        try {
            cost.set(Double.parseDouble(fields[4]));
        } catch (NumberFormatException e) {
            return;
        }
        context.write(store, cost);
    }
}
