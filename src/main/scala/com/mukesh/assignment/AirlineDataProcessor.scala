package com.mukesh.assignment
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.Month

object AirlineDataProcessor {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KLMDataAnalyzer")
      .master("local[*]")
      .getOrCreate()
    //Created custom schema to process the data
    val customSchemaForAirLineData = StructType(Array(
      StructField("ActualElapsedTime", IntegerType, nullable = true),
      StructField("AirTime", IntegerType, nullable = true),
      StructField("ArrDelay", DoubleType, nullable = true),
      StructField("ArrTime", StringType, nullable = true), // Example: Changing data type to StringT
      StructField("CRSArrTime", IntegerType, nullable = true),
      StructField("CRSDepTime", IntegerType, nullable = true),
      StructField("CRSElapsedTime", IntegerType, nullable = true),
      StructField("CancellationCode", StringType, nullable = true),
      StructField("Cancelled", IntegerType, nullable = true),
      StructField("CarrierDelay", DoubleType, nullable = true),
      StructField("DayOfWeek", IntegerType, nullable = true),
      StructField("DayofMonth", IntegerType, nullable = true),
      StructField("DepDelay", DoubleType, nullable = true),
      StructField("DepTime", StringType, nullable = true),
      StructField("Dest", StringType, nullable = true),
      StructField("Distance", DoubleType, nullable = true),
      StructField("Diverted", IntegerType, nullable = true),
      StructField("FlightNum", IntegerType, nullable = true),
      StructField("LateAircraftDelay", DoubleType, nullable = true),
      StructField("Month", IntegerType, nullable = true),
      StructField("NASDelay", DoubleType, nullable = true),
      StructField("Origin", StringType, nullable = true),
      StructField("SecurityDelay", DoubleType, nullable = true),
      StructField("TailNum", StringType, nullable = true),
      StructField("TaxiIn", IntegerType, nullable = true),
      StructField("TaxiOut", IntegerType, nullable = true),
      StructField("UniqueCarrier", StringType, nullable = true),
      StructField("WeatherDelay", DoubleType, nullable = true),
      StructField("Year", IntegerType, nullable = true)
    ))
    //Read the data frame Present in the Local system
    val df_airline_dataset = spark.read
      .format("csv")
      .option("header", "true")
      .schema(customSchemaForAirLineData)
      .load("C:\\Users\\a885945\\Downloads\\airline-dataset.csv")

    val averageDelayByCarrier = df_airline_dataset
      .groupBy("UniqueCarrier")
      .agg(
        (sum("ArrDelay") + sum("DepDelay") + sum("CarrierDelay") + sum("WeatherDelay") + sum("NASDelay") + sum("SecurityDelay") + sum("LateAircraftDelay")) / lit(7) as "AverageDelay"
      )

    // Filtered out null values and find the max and min delay for Aircraft
    val maxDelayTime = averageDelayByCarrier.filter(col("AverageDelay").isNotNull).select(max("AverageDelay")).head().getDouble(0)
    val maxDelayByCarrier = averageDelayByCarrier.select(col("UniqueCarrier")).head().getString(0)
    val minDelayTime = averageDelayByCarrier.filter(col("AverageDelay").isNotNull).select(min("AverageDelay")).head().getDouble(0)
    val minDelayByCarrier = averageDelayByCarrier.select(col("UniqueCarrier")).orderBy(col("AverageDelay")).head().getString(0)

    println(s"The carrier with the highest average delay time is $maxDelayByCarrier with an average delay of $maxDelayTime.")
    println(s"The carrier with the lowest average delay time is $minDelayByCarrier with an average delay of $minDelayTime.")
    val totalDelay = df_airline_dataset
      .selectExpr("UniqueCarrier", "ArrDelay + DepDelay + CarrierDelay + WeatherDelay + NASDelay + SecurityDelay + LateAircraftDelay as TotalDelay")

    /* Calculate correlation between distance and total delay
    The corellation defined as
    a. 1 indicates a perfect positive linear relationship,
    b. -1 indicates a perfect negative linear relationship,
    c. 0 indicates no linear relationship.
     */
    val correlationDistanceArrDelay = df_airline_dataset
      .join(totalDelay, df_airline_dataset("UniqueCarrier") === totalDelay("UniqueCarrier"))
      .stat
      .corr("Distance", "TotalDelay")

    if (correlationDistanceArrDelay == 0) {
      println("It indicated that there is no corelation between Distance and Arrival Delay")
    }
    else if (correlationDistanceArrDelay > 0 & correlationDistanceArrDelay <= 1) {
      println("It shows that there is a strong corelation exists between Distance and Arrival Delay")
    }
    else {
      println("It shows there is very weeak corelation exists between Distance and Arrival Delay")
    }

    /*
    1. Grouping the data by Year
    2.Aggregating cancellations and total flights
    3.Calculating the cancellation percentage for each year
    4.Ordering the result DataFrame by year
     */
    val flightsAndCancellationsOverYears = df_airline_dataset
      .groupBy("Year")
      .agg(
        count(when(col("Cancelled") === "1", true)).alias("Cancellations"),
        count("*").alias("TotalFlights")
      )
      .withColumn("CancellationPercentage", col("Cancellations") / col("TotalFlights") * 100)
      .orderBy("Year")

    val maxPercentageFlightCancellation = flightsAndCancellationsOverYears.filter(col("CancellationPercentage").isNotNull).select(max("CancellationPercentage")).head().getDouble(0)
    val maxCancellationInYear = flightsAndCancellationsOverYears.select(col("Year")).head().getInt(0)
    val minPercentageFlightCancellation = flightsAndCancellationsOverYears.filter(col("CancellationPercentage").isNotNull).select(min("CancellationPercentage")).head().getDouble(0)
    val minCancellationInYear = flightsAndCancellationsOverYears.select(col("Year")).orderBy(col("CancellationPercentage")).head().getInt(0)

    println("After Analyzing the data i have got insight as given below-")
    println(s"The year with the Highest percentage of flight cancellation is $maxCancellationInYear with percentage  of $maxPercentageFlightCancellation.")
    println(s"The year with the Lowest percentage of flight cancellation is $minCancellationInYear  with percentage  of $minPercentageFlightCancellation.")

    val delaysByAirport = df_airline_dataset
      .groupBy("Origin")
      .agg(
        //Counting delays where either LateAircraftDelay > 0 or flight is Cancelled
        sum(when(col("LateAircraftDelay") > 0 || col("Cancelled") == 1, 1).otherwise(0)).alias("TotalDelaysByAircraft")
      )
      //Ordering the result  by total delays and selecting the airport with the least delays
      .orderBy("TotalDelaysByAircraft")
      .limit(1)

    // Extracting values from the DataFrame
    val airport = delaysByAirport.select("Origin").head().getString(0)
    val totalDelays = delaysByAirport.select("TotalDelaysByAircraft").head().getLong(0)

    println(s"The airport $airport has the highest number of delays/cancellations: $totalDelays")

    /*
    If the value of the "DayOfWeek" column is 1 (Sunday) or 7 (Saturday), the flight is marked as "Weekend",
       otherwise, it's marked as "Weekday"
    */
    val df_with_weekday_indicator = df_airline_dataset.withColumn("IsWeekend", when(col("DayOfWeek").isin(1, 7), "Weekend").otherwise("Weekday"))

    val averageDelayByCarrierAndDayType = df_with_weekday_indicator
      .groupBy("IsWeekend")
      .agg(
        (sum("ArrDelay") + sum("DepDelay") + sum("CarrierDelay") + sum("WeatherDelay") +
          sum("NASDelay") + sum("SecurityDelay") + sum("LateAircraftDelay")) / lit(7) as "AverageDelay"
      )

    // Extracting values from the DataFrame
    val weekdayDelay = averageDelayByCarrierAndDayType.filter(col("IsWeekend") === "Weekday").select("AverageDelay").head().getDouble(0)
    val weekendDelay = averageDelayByCarrierAndDayType.filter(col("IsWeekend") === "Weekend").select("AverageDelay").head().getDouble(0)

    if (weekdayDelay > weekendDelay) {
      println(s"Weekday delay is more than weekend delay :$weekdayDelay")
    }
    else if (weekendDelay > weekdayDelay) {
      println(s"Weekend delay is more than Weekday delay $weekendDelay")
    }
    else {
      println("Both the delay time between weekday and weekend are same")
    }

    val cancellationsByMonth = df_airline_dataset
      .filter(col("Cancelled") === "1")
      .groupBy("Month")
      .agg(count("*").alias("TotalCancellations"))
      .orderBy(desc("TotalCancellations"))
      // Selecting the month with the highest cancellations
      .limit(1)
      // Retrieving the first row which represents the month with the highest cancellations
      .first()

    val monthWithHighestCancellations = cancellationsByMonth.getInt(0)
    val totalCancellations = cancellationsByMonth.getLong(1)
    val monthName = Month.of(monthWithHighestCancellations).toString
    println(s"The Month which have Highest number of Flight cancellation is $monthName,The Number of cancellation is $totalCancellations")

    val cancellationReasons = df_airline_dataset
      //Filtered out the null and NA records from dataset
      .filter(col("CancellationCode").isNotNull && col("CancellationCode") =!= "NA")
      .groupBy("CancellationCode")
      .agg(
        count("*").alias("TotalCancellations"),
        (count("*") / df_airline_dataset.filter(col("CancellationCode").isNotNull && col("CancellationCode") =!= "NA").count() * 100).alias("Percentage")
      )
      .orderBy(desc("Percentage"))
    cancellationReasons
      .select("CancellationCode", "TotalCancellations")
      .take(3)
      .foreach { row =>  // Printed the first 3 reason for most flight cancellation
        val reason = row.getString(0)
        val count = row.getLong(1)
        println(s"Due to Reason $reason, the number of flight cancellations is $count")
      }

    val dfWithTotalTaxiTime = df_airline_dataset
      .withColumn("TotalTaxiTime", col("TaxiIn") + col("TaxiOut"))

    // Calculate the correlation between distance and total taxi time
    val correlationDistanceTaxiTime = dfWithTotalTaxiTime.stat.corr("Distance", "TotalTaxiTime")
    if (correlationDistanceTaxiTime == 0) {
      println("There is no co relation between  length of the flight and the amount of time spent taxiing")
    }
    else if (correlationDistanceTaxiTime > 0 & correlationDistanceTaxiTime <= 1) {
      println("There is a strong corelation exists between length of the flight and correlationDistanceTaxiTime")
    }
    else {
      println("It shows that there is very low corelaion exists between length of the flight and correlationDistanceTaxiTime")
    }
    // Calculate the percentage of delayed flights for each airport
    val delayedFlightsByAirport = df_airline_dataset
      .groupBy("Origin")
      .agg(
        (sum(when(col("ArrDelay") > 0, 1).otherwise(0)) / count("*") * 100).alias("PercentageDelayedFlights")
      )
      .orderBy(desc("PercentageDelayedFlights"))
    // Find the airport with the highest percentage of delayed flights
    val highestDelayAirport = delayedFlightsByAirport.head()
    println(s"The airport with the highest percentage of delayed flights is ${highestDelayAirport.getString(0)} with a percentage of ${highestDelayAirport.getDouble(1)}")

    // Find the airport with the lowest percentage of delayed flights
    val lowestDelayAirport = delayedFlightsByAirport.orderBy("PercentageDelayedFlights").head()
    println(s"The airport with the lowest percentage of delayed flights is ${lowestDelayAirport.getString(0)} with a percentage of ${lowestDelayAirport.getDouble(1)}")

    /*
    For use case number 12
    1. First i have calculated FirFlyDate of Aircraft by taking min(Month,DayofMonth,year)
    2.Join the data with original Airline dataset
    3.Found the Flight from Joined dataframe
    4. Calculate the age by substracting FlightDate and First fly date
    5.Grouping the data frame Age
    6. Extract the insight from the data frame
     */


    val flightDateDF = df_airline_dataset
      .groupBy("FlightNum")
      .agg(
        min("Month").alias("MinMonth"),
        min("DayofMonth").alias("MinDayOfMonth"),
        min("Year").alias("MinYear")
      )
      .withColumn("First_Fly_Date",
        concat_ws("-", col("MinYear"), col("MinMonth"), col("MinDayOfMonth").cast("string"))
      )

    val joinedDF = df_airline_dataset.join(flightDateDF, Seq("FlightNum"), "inner")

    val datedDF = joinedDF.withColumn("FlightDate",
      concat_ws("-", col("Year"), col("Month"), col("DayOfMonth").cast("string"))
    )

    val agedDF = datedDF.withColumn("AgeInMonths",
      months_between(to_date(col("FlightDate")), to_date(col("First_Fly_Date")))
    )


    //Compute the Average late Aircraft delay by the Aircraft Age in month
    val finalDFforInsight = agedDF.groupBy("AgeInMonths").agg(
      avg("LateAircraftDelay").alias("AvgLateAircraftDelay")
    )


    // Find the age with the maximum delay along with the delay value
    val maxDelayRow = finalDFforInsight.orderBy(desc("AvgLateAircraftDelay")).first()
    val maxDelayAge = maxDelayRow.getDouble(0)
    val maxDelayValue = maxDelayRow.getDouble(1)
    println(s"The age at which the maximum average late aircraft delay occurred is: $maxDelayAge months with a delay of $maxDelayValue")

    // Find the age with the minimum delay along with the delay value
    val minDelayRow = finalDFforInsight.orderBy("AvgLateAircraftDelay").first()
    val minDelayAge = minDelayRow.getDouble(0)
    val minDelayValue = minDelayRow.getDouble(1)
    println(s"The age at which the minimum average late aircraft delay occurred is: $minDelayAge months with a delay of $minDelayValue")

    //Found out different distribution on airline data
    val delayDistributionByAirline = df_airline_dataset
      .groupBy("UniqueCarrier")
      .agg(
        avg(when(col("ArrDelay") > 0, col("ArrDelay"))).alias("MeanDelay"),
        stddev(when(col("ArrDelay") > 0, col("ArrDelay"))).alias("StdDevDelay"),
        min(when(col("ArrDelay") > 0, col("ArrDelay"))).alias("MinDelay"),
        max(when(col("ArrDelay") > 0, col("ArrDelay"))).alias("MaxDelay"),

      )

    val minDelayInfo = delayDistributionByAirline.orderBy("MinDelay").selectExpr("UniqueCarrier", "MinDelay").first()
    val maxDelayInfo = delayDistributionByAirline.orderBy(desc("MaxDelay")).selectExpr("UniqueCarrier", "MaxDelay").first()
    val avgDelayInfo = delayDistributionByAirline.orderBy("MeanDelay").selectExpr("UniqueCarrier", "MeanDelay").first()

    println("After Analyse the above data i have found the insights as follows")
    println(s"Carrier with the minimum delay: " + minDelayInfo)
    println(s"Carrier with the maximum delay: " + maxDelayInfo)
    println(s"Carrier with the average delay: " + avgDelayInfo)

    //Filtered the null data from the dataset
    val filteredAirlineData = df_airline_dataset.na.drop("any", Seq("WeatherDelay", "FlightNum"))

    //Find the corelation between two columns
    val correlation = filteredAirlineData.stat.corr("WeatherDelay", "FlightNum")

    if (correlation > 0 & correlation <= 1) {
      println("There is a strong correlation between WeatherDelay and FlightNumber")
    }
    else if (correlation == 0) {
      println("It shows that there is no corelation between Weather delay and FlightNumber")
    }
    else {
      println("There is very week corelation between Weather delay and flight number")
    }
  }

}
