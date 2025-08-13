package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("TotalVentesParVilleEtAnnee")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> ventes = sc.textFile("ventes.txt");

        // (ville-année, prix)
        JavaPairRDD<String, Double> ventesParVilleEtAnnee = ventes.mapToPair(line -> {
            String[] parts = line.split("\\s+");
            String date = parts[0];
            String ville = parts[1];
            double prix = Double.parseDouble(parts[3]);

            // Extraire l'année depuis la date "YYYY-MM-DD"
            String annee = date.split("-")[0];

            String cle = ville + "-" + annee;
            return new Tuple2<>(cle, prix);
        });

        // Total par (ville, année)
        JavaPairRDD<String, Double> totalParVilleEtAnnee = ventesParVilleEtAnnee.reduceByKey(Double::sum);

        totalParVilleEtAnnee.collect().forEach(result ->
                System.out.println(result._1 + " -> " + result._2)
        );

        sc.close();
    }
}
