# Databricks notebook source
# MAGIC %md
# MAGIC ### Day 4.   Key - Value 

# COMMAND ----------

#create an RDD
palabras = sc.parallelize(['HOLA', 'Que', 'TAL', 'Bien'])

#applying transformation map
pal_long = palabras.map(lambda elemento: (elemento, len(elemento)))

#print the result
pal_long.collect()

# COMMAND ----------

#create an RDD 
r = sc.parallelize([('A',1),('C',4),('A',1),('B',1),('B',4)])

#applying the transformation
rr = r.reduceByKey(lambda v1, v2: v1 + v2)

#print the result
print(r.collect()) #sample
print(rr.collect()) #result


# COMMAND ----------

#create an RDD 
r = sc.parallelize([('A',1),('C',4),('A',1),('B',1),('B',4)])

#applying the transformation
rr = r.reduceByKey(lambda v1, v2: str(v1 + v2))

#print the result
print(r.collect()) #sample
print(rr.collect()) #result



# COMMAND ----------

# MAGIC %md
# MAGIC #### groupByKey()

# COMMAND ----------

r = sc.parallelize([('A',1),('C',2),('A',3),('B',4),('B',5)])

#print the result
print(r.groupByKey().mapValues(list).collect())
print(r.groupByKey().mapValues(len).collect())



# COMMAND ----------

# MAGIC %md
# MAGIC #### sortByKey()

# COMMAND ----------

rdd = sc.parallelize([('A',1),('B',2),('C',3),('A',4),('A',5), ('B',6)])

#create new RDD
res = rdd.sortByKey(True) #sort ascending. if we put (False) sort descending

#print the results
print(rdd.collect())
print(res.collect())


# COMMAND ----------

# MAGIC %md
# MAGIC #### join()

# COMMAND ----------

rdd1 = sc.parallelize([('A',1),('B',2),('C',3)])
rdd2 = sc.parallelize([('A',4),('B',5),('C',6),('D',7)])

rddjoin = rdd1.join(rdd2)

rddleftOuterJoin = rdd1.leftOuterJoin(rdd2)
rddrightOuterJoin = rdd1.rightOuterJoin(rdd2)
rddfullOuterJoin = rdd1.fullOuterJoin(rdd2)

#print join()
print(rddjoin.collect())

#print leftOuterJoin. Print all the values of rdd1 and all that match rdd2
print(rddleftOuterJoin.collect())

#print rightOuterJoin. Print all the values of rdd2 and all that match rdd1
print(rddrightOuterJoin.collect())

#print fullOuterJoin. Print all the values of rdd1 and rdd2. It doesn't matter if they match or not.
print(rddfullOuterJoin.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4
# MAGIC ###### Group sales by brand in a quarter. Compare the total sales of the previous year

# COMMAND ----------

#create first RDD
rdd1 = sc.parallelize([('Nike',51805),('Puma',42329),('Adidas',63542),('Puma',27943),('Nike',75335),('Puma',45102),('Adidas',63583)])


#create second RDD
rdd2 = sc.parallelize([('Nike',2224589),('Adidas',219123),('Puma',166524)])

#sales from each one
sales1 = rdd1.reduceByKey(lambda v1, v2: v1 + v2)
sales2 = rdd2.reduceByKey(lambda v1, v2: v1 + v2)

#compare total sales last year
print(ventas1.collect())
print(ventas2.collect())

