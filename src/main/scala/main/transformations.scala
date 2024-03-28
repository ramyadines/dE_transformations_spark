import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder
  .appName("Analysis")
  .getOrCreate()

// Read JSON files into DataFrame
val df = spark.read.json("data/*.json")

// Create temporary view
df.createOrReplaceTempView("donations")

// Calculate required metrics using Spark SQL queries
val totalDonations = spark.sql("SELECT COUNT(*) AS total_donations FROM donations").collect()(0)(0).asInstanceOf[Long]
val uniqueIndividuals = spark.sql("SELECT COUNT(DISTINCT customer_id) AS unique_individuals FROM donations").collect()(0)(0).asInstanceOf[Long]
val totalDonationAmount = spark.sql("SELECT SUM(donation.amount) AS total_donation_amount FROM donations").collect()(0)(0).asInstanceOf[Double]
val averageDonationAmount = totalDonationAmount / totalDonations
val failedDonations = spark.sql("SELECT COUNT(*) AS failed_donations FROM donations WHERE donation.status = 'failed'").collect()(0)(0).asInstanceOf[Long]

// Calculate proportion of donations from London vs outside London
val londonDonations = spark.sql("SELECT COUNT(*) AS london_donations FROM donations WHERE customer_profile.region = 'London'").collect()(0)(0).asInstanceOf[Long]
val totalLondonDonations = totalDonations - londonDonations
val proportionLondonDonations = londonDonations.toDouble / totalDonations

// Print results
println("Number of unique individuals: " + uniqueIndividuals)
println("Total donation amount: " + totalDonationAmount)
println("Average donation amount: " + averageDonationAmount)
println("Number of failed donations: " + failedDonations)
println("Proportion of donations from London: " + proportionLondonDonations)

// Stop SparkSession
spark.stop()
