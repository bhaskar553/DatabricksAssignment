# Databricks notebook source
from pyspark.sql.functions import expr, col
import pyspark.sql.functions as fn
sampleEmployee = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/shared_uploads/bhaskar.dakoori072@gmail.com/us_500.csv")

# COMMAND ----------

employeeDF = sampleEmployee.withColumn('web', expr('explode(array_repeat(web,100))'))

# COMMAND ----------

employeeDF_grouped = employeeDF.groupby(['city'])
CityEmployeeDensity = employeeDF_grouped.agg(fn.count(col('email')).alias('countOfEmployees'))

# COMMAND ----------

employeeDF.createOrReplaceTempView("employeeDataFrame")
CityEmployeeDensity.createOrReplaceTempView("CityEmpDensity")

sequenceOfCityDF = sqlContext.sql(" select city, countOfEmployees, rank() over(order by countOfEmployees desc, city) as Sequence from CityEmpDensity ")
sequenceOfCityDF.createOrReplaceTempView("sequenceOfCityDataFrame")

VaccinationDrivePlan = sqlContext.sql(" SELECT EDF.*, SDF.Sequence FROM employeeDataFrame EDF INNER JOIN sequenceOfCityDataFrame SDF ON EDF.city = SDF.city ")
VaccinationDrivePlan.show()

# COMMAND ----------

VaccinationDrivePlan.createOrReplaceTempView("VaccinationlPlan")
noOfDaysVaccineDrive = sqlContext.sql("SELECT city, countOfEmployees, CEILING(countOfEmployees/100) as noOfDaysToCompleteVaccination FROM CityEmpDensity")

filnalVaccineDrive = noOfDaysVaccineDrive.withColumn('noOfDaysToCompleteVaccination', expr('explode(array_repeat(noOfDaysToCompleteVaccination,int(noOfDaysToCompleteVaccination)))'))
filnalVaccineDrive.createOrReplaceTempView("filnalVaccineDrive")

# COMMAND ----------

filnalVaccineSchedule_Sequential = sqlContext.sql("SELECT city,countOfEmployees AS countOfEmployeesOfCity, current_date() + ROW_NUMBER() OVER(order by countOfEmployees desc ) - 1 AS VaccineScheduleDate FROM filnalVaccineDrive")
filnalVaccineSchedule_Sequential.show()

# COMMAND ----------

filnalVaccineSchedule_Parallel = sqlContext.sql("SELECT city,countOfEmployees AS countOfEmployeesOfCity, current_date() + ROW_NUMBER() OVER(partition by city order by countOfEmployees desc ) - 1 AS VaccineScheduleDate FROM filnalVaccineDrive")
filnalVaccineSchedule_Parallel.show()

# COMMAND ----------

noOfDaysVaccineDriveForCity = noOfDaysVaccineDrive
noOfDaysVaccineDriveForCity.show()


# COMMAND ----------


