package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

public class App2TotalVentesVilleAnnee {

    public static void main(String[] args) {

        // --- Configuration Spark ---
        SparkConf configuration = new SparkConf()
                .setAppName("VentesVilleAnnee")
                .setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(configuration);

        SparkSession session = SparkSession.builder()
                .appName("AnalyseVentes")
                .getOrCreate();

        // --- Définition du schéma pour le fichier ---
        StructType venteSchema = new StructType()
                .add("date", DataTypes.StringType)
                .add("ville", DataTypes.StringType)
                .add("produit", DataTypes.StringType)
                .add("prix", DataTypes.DoubleType);

        // --- Chargement du fichier ---
        Dataset<Row> ventesDF = session.read()
                .option("delimiter", " ")
                .schema(venteSchema)
                .csv("ventes.txt");

        // --- Extraction de l'année ---
        Dataset<Row> ventesAvecAnnee = ventesDF.withColumn(
                "annee",
                functions.year(functions.to_date(ventesDF.col("date"), "yyyy-MM-dd"))
        );

        // --- Vue temporaire pour SQL ---
        ventesAvecAnnee.createOrReplaceTempView("tblVentes");

        // --- Requête SQL ---
        Dataset<Row> resultat = session.sql(
                "SELECT ville, annee, SUM(prix) AS montant_total " +
                "FROM tblVentes " +
                "GROUP BY ville, annee " +
                "ORDER BY annee ASC, ville ASC"
        );

        // --- Affichage ---
        System.out.println("\n=== VENTES TOTALES PAR VILLE ET PAR ANNÉE ===\n");
        resultat.show();

        // --- Nettoyage ---
        jsc.close();
        session.close();
    }
}
