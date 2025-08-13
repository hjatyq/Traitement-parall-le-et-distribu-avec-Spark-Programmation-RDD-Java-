package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App1 {
    public static void main(String[] args) {

        // 1. Configuration Spark
        SparkConf conf = new SparkConf()
                .setAppName("TotalVentesParVille")
                .setMaster("local[*]"); // Mode local pour tester

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2. Charger le fichier
        JavaRDD<String> ventes = sc.textFile("ventes.txt");

        // 3. Transformation en paires (ville, prix)
        JavaPairRDD<String, Double> ventesParVille = ventes.mapToPair(line -> {
            String[] parts = line.split("\\s+"); // Séparation par espace(s)
            String ville = parts[1];
            double prix = Double.parseDouble(parts[3]);
            return new Tuple2<>(ville, prix);
        });

        // 4. Calcul total par ville
        JavaPairRDD<String, Double> totalParVille = ventesParVille.reduceByKey((v1, v2) -> v1 + v2);

        // 5. Affichage des résultats
        totalParVille.collect().forEach(result ->
                System.out.println(result._1 + " -> " + result._2)
        );

        sc.close();
    }
}
