package ma.atif;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App2TotalVentesParVilleEtAnnee {

    public static void main(String[] args) {

        // Config
        SparkConf configuration = new SparkConf()
                .setAppName("VentesParVilleEtAnnee")
                .setMaster("local[*]");

        JavaSparkContext context = new JavaSparkContext(configuration);

        // Load file ventes2.txt
        JavaRDD<String> fileData = context.textFile("ventes2.txt");

        // (ville, année) -> montant
        JavaPairRDD<Tuple2<String, Integer>, Double> ventesVilleAnnee = fileData
                .filter(line -> !line.trim().isEmpty())
                .mapToPair(line -> {
                    String[] champs = line.split("\\s+");

                    // Safety check
                    if (champs.length < 4) {
                        return new Tuple2<>(new Tuple2<>("UNKNOWN", 0), 0.0);
                    }

                    // Extract fields
                    String date = champs[0];               // example: 2022-01-03
                    String ville = champs[1];              // Casablanca
                    double montant = Double.parseDouble(champs[3]); // 3500

                    int annee = Integer.parseInt(date.split("-")[0]); // 2022

                    // Return ((ville, année), montant)
                    return new Tuple2<>(
                            new Tuple2<>(ville, annee),
                            montant
                    );
                });

        // Reduce By Key
        JavaPairRDD<Tuple2<String, Integer>, Double> resultats =
                ventesVilleAnnee.reduceByKey(Double::sum);

        // Output
        System.out.println("\n=== TOTAL DES VENTES PAR VILLE ET ANNÉE ===\n");
        resultats.collect().forEach(tuple ->
                System.out.println(
                        tuple._1()._1() + " - " + tuple._1()._2() +
                                " -> " + tuple._2()
                )
        );

        context.close();
    }
}
