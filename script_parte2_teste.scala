val n1 = spark.read.format("csv").option("header","false").option("delimiter"," ").load("file:///home/jjunior/Downloads/NASA_access_log_Jul95.gz")
val n2 = spark.read.format("csv").option("header","false").option("delimiter"," ").load("file:///home/jjunior/Downloads/NASA_access_log_Aug95.gz ")


val nasalog = n1.union(n2)
nasalog.cache()

println("Hosts Unicos")
nasalog.agg(countDistinct("_c0")).show()
/*


+-------------------+
|count(DISTINCT _c0)|
+-------------------+
|             137979|
+-------------------+




*/

println("Quantidade de erros 404")
nasalog.where(nasalog("_c6") ==="404").count()

/*
Long = 20871
*/


println("Hosts que mais deram erro 404")
nasalog.select("_c0", "_c6").where(nasalog("_c6") ==="404").groupBy("_c0").count().sort(desc("count")).limit(5).show()
/*+--------------------+-----+
|                 _c0|count|
+--------------------+-----+
|hoohoo.ncsa.uiuc.edu|  251|
|piweba3y.prodigy.com|  156|
|jbiagioni.npt.nuw...|  132|
|piweba1y.prodigy.com|  114|
|www-d4.proxy.aol.com|   91|
+--------------------+-----+
*/

println("Quantidade de 404 por dia")
val dfDia404=nasalog.select(substring(col("_c3"),2,11).as("dia"),nasalog("_c6")).where(nasalog("_c6") ==="404").groupBy("dia").count().sort(asc("dia")).show

dfDia404.write.format("com.databricks.spark.csv").save("/home/jjunior/Downloads/404_porDia.csv")




println("Total de bytes enviados")
nasalog.agg(sum("_c7")).show
/*
+---------------+
|       sum(_c7)|
+---------------+
|6.5524319796E10|
+---------------+
*/








