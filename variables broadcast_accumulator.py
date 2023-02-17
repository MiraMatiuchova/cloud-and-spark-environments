# Databricks notebook source
# MAGIC %md
# MAGIC ## Variable Broadcast

# COMMAND ----------

#we prepare the work environment
import pyspark
from pyspark.sql import SparkSession

# COMMAND ----------

#create spark constructor
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

states = {'NY':'New York','CA':'California','FL':'Flprida'}

broadcastStates = spark.sparkContext.broadcast(states)

data = [('James' , 'Smith', 'USA', 'CA'),
        ('Michael' , 'Rose', 'USA', 'NY'),
        ('Robert' , 'WIlliams', 'USA', 'CA'),
         ('Maria' , 'Jones', 'USA', 'FL')]

rdd = spark.sparkContext.parallelize(data)

def state_convert(code):
    return broadcatStates.value[code]

result = rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).collect

print(result)     
        

# COMMAND ----------

# MAGIC %md
# MAGIC ###Variable Accumulator 

# COMMAND ----------

#create an accumulator
acum = sc.accumulator(0)
print('Valor inicial:', acum)

rdd = sc.parallelize([1,2,3,4])
rdd.foreach(lambda x: acum.add(x))

print('Valor final:', acum.value)
