package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App1TotalVentesParVille {

    public static void main(String[] args) {

        // Configuration Spark
        SparkConf configuration = new SparkConf()
                .setAppName("VentesParVille")
                .setMaster("local[*]");

        JavaSparkContext context = new JavaSparkContext(configuration);

        // Charger le fichier d'entrée
        JavaRDD<String> fileData = context.textFile("ventes.txt");

        // Transformer chaque ligne en un tuple (ville, prix)
        JavaPairRDD<String, Double> ventesParVille = fileData
                .filter(line -> !line.trim().isEmpty()) // ignorer lignes vides
                .mapToPair(line -> {
                    String[] champs = line.split("\\s+");

                    // sécurité si la ligne n'est pas correcte
                    if (champs.length < 4) {
                        return new Tuple2<>("UNKNOWN", 0.0);
                    }

                    String ville = champs[1];
                    double montant = Double.parseDouble(champs[3]);

                    return new Tuple2<>(ville, montant);
                });

        // Calculer la somme totale des ventes par ville
        JavaPairRDD<String, Double> resultats = ventesParVille
                .reduceByKey((a, b) -> a + b);

        // Affichage final
        System.out.println("\n=== TOTAL DES VENTES PAR VILLE ===\n");
        resultats.collect().forEach(tuple ->
                System.out.println(tuple._1() + " -> " + tuple._2())
        );

        context.close();
    }
}
