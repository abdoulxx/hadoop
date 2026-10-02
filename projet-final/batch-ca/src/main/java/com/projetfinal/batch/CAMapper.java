package com.projetfinal.batch;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Colonnes du TSV (online_retail.tsv) :
 * InvoiceNo StockCode Description Quantity InvoiceDate UnitPrice CustomerID Country
 * Clé émise : "Country\tYYYY-MM", valeur : Quantity * UnitPrice.
 */
public class CAMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private final Text paysMois = new Text();
    private final DoubleWritable ca = new DoubleWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("InvoiceNo\t")) {
            return; // en-tete
        }
        String[] fields = line.split("\t", -1);
        if (fields.length < 8) {
            return;
        }
        String invoiceDate = fields[4];
        String country = fields[7];
        if (invoiceDate.length() < 7 || country.isEmpty()) {
            return;
        }
        String mois = invoiceDate.substring(0, 7); // "YYYY-MM"

        double quantity;
        double unitPrice;
        try {
            quantity = Double.parseDouble(fields[3]);
            unitPrice = Double.parseDouble(fields[5]);
        } catch (NumberFormatException e) {
            return;
        }

        paysMois.set(country + "\t" + mois);
        ca.set(quantity * unitPrice);
        context.write(paysMois, ca);
    }
}
