

## Exercice 1 (Traitement des ventes avec Spark / Java)

**Résumé rapide**
Dans cet exercice nous avons créé 2 petites applications Spark en Java (RDD API) qui lisent un fichier texte `ventes.txt` (format `YYYY-MM-DD Ville Produit Montant`) et calculent :

1. Le **total des ventes par ville** (`App1TotalVentesParVille`)
2. Le **total des ventes par ville et par année** (`App2TotalVentesParVilleEtAnnee`)

Nous avons exécuté et testé les programmes en local (IntelliJ) avec **Java 11** et Spark 3.5.1.

---

## Arborescence et fichiers créés

```
project-root/
├─ pom.xml
├─ src/
│  └─ main/
│     └─ java/
│        └─ ma/
│           └─ atif/
│              ├─ App1TotalVentesParVille.java
│              └─ App2TotalVentesParVilleEtAnnee.java
├─ ventes.txt      <-- fichier d'entrée principal (existant)
└─ ventes2.txt     <-- second fichier d'entrée (nouveau - exemple)
```

#### Contenu (exemples)

`ventes.txt`

```
2022-01-03 Casablanca TV 3500
2022-01-04 Rabat PC 7000
2022-03-12 Casablanca Smartphone 2500
2023-05-12 Marrakech TV 4000
2023-05-13 Rabat Smartphone 2000
2023-07-20 Casablanca PC 6500
```

`ventes2.txt` (exemple fourni)

```
2021-02-11 Tanger TV 4500
2021-06-20 Casablanca Laptop 8000
2022-01-03 Rabat Smartphone 3000
2022-02-18 Tanger Laptop 5000
2023-03-15 Agadir TV 3500
2023-07-10 Rabat PC 6000
2024-01-05 Casablanca Smartphone 2500
2024-03-20 Marrakech PC 7000
```

---

## Technologies et bibliothèques utilisées

* **Java 11** (OpenJDK / Temurin) — obligatoire pour compatibilité avec Spark 3.5.x.
* **Apache Spark 3.5.1** (spark-core_2.12 et spark-sql_2.12 dans le `pom.xml`).
* **Maven** pour le build.
* **IntelliJ IDEA** pour le développement et l’exécution locale.
* **SLF4J / Log4j** pour les logs (Spark embarque ses dépendances).

---

## Code principal (rappel)

* `App1TotalVentesParVille.java` — lit `ventes.txt`, map `(ville -> montant)` puis `reduceByKey(sum)` et affiche le résultat.
* `App2TotalVentesParVilleEtAnnee.java` — lit `ventes2.txt`, extrait l’année depuis la date (`YYYY-MM-DD`), map `((ville, annee) -> montant)` puis `reduceByKey(sum)` et affiche.

(Le code utilise `JavaSparkContext` et RDD API comme demandé.)

---

## Comment exécuter (IntelliJ)

1. Vérifier que **Project SDK** et **Module SDK** sont configurés sur **Java 11** :
   `File → Project Structure → Project SDK` → sélectionner JDK 11 (Temurin-11.0.29 dans ton cas).
2. Vérifier la configuration d’exécution :
   `Run → Edit Configurations…` → sélectionne la configuration Java et assure-toi que la **runtime JRE** (ou JDK) est Java 11.
3. Dans le code, tu peux utiliser un chemin relatif (ex. `"ventes.txt"`) si le fichier est à la racine du projet. Si IntelliJ ne trouve pas le fichier, utiliser le chemin absolu Windows (ex. `C:/Users/pc/Desktop/.../ventes.txt`).
4. Run → Run `App1TotalVentesParVille` / `App2TotalVentesParVilleEtAnnee`.

---


## Exemples de sortie (console)

`App1TotalVentesParVille` → affichage

```
=== TOTAL DES VENTES PAR VILLE ===

Marrakech -> 4000.0
Rabat -> 9000.0
Casablanca -> 12500.0
```

`App2TotalVentesParVilleEtAnnee` → affichage

```
=== TOTAL DES VENTES PAR VILLE ET ANNÉE ===

Marrakech - 2023 -> 4000.0
Casablanca - 2022 -> 6000.0
Rabat - 2022 -> 7000.0
Rabat - 2023 -> 2000.0
Casablanca - 2023 -> 6500.0
```

