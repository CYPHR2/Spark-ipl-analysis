Python 3.8.2 (default, Jul 16 2020, 14:00:26) 
[GCC 9.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
21/08/26 03:59:23 WARN Utils: Your hostname, sunil-VirtualBox resolves to a loopback address: 127.0.1.1; using 10.0.2.15 instead (on interface enp0s3)
21/08/26 03:59:23 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/spark/jars/spark-unsafe_2.12-3.1.2.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
21/08/26 03:59:24 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
21/08/26 03:59:29 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.1.2
      /_/

Using Python version 3.8.2 (default, Jul 16 2020 14:00:26)
Spark context Web UI available at http://10.0.2.15:4041
Spark context available as 'sc' (master = local[*], app id = local-1629930569661).
SparkSession available as 'spark'.
>>> var = spark.read.json("sample.json", multiLine = "true")
[Stage 0:>                                                          (0 + 1) / 1                                                                               21/08/26 04:01:34 WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
>>> from pyspark.sql.functions import *
>>> 
>>> 
>>> 
>>> var = spark.read.json("sample.json", multiLine = "true")
>>> var.printSchema()
root
 |-- info: struct (nullable = true)
 |    |-- balls_per_over: long (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- dates: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- event: struct (nullable = true)
 |    |    |-- match_number: long (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |-- gender: string (nullable = true)
 |    |-- match_type: string (nullable = true)
 |    |-- officials: struct (nullable = true)
 |    |    |-- match_referees: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- reserve_umpires: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- tv_umpires: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- umpires: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |-- outcome: struct (nullable = true)
 |    |    |-- by: struct (nullable = true)
 |    |    |    |-- runs: long (nullable = true)
 |    |    |-- winner: string (nullable = true)
 |    |-- overs: long (nullable = true)
 |    |-- player_of_match: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- players: struct (nullable = true)
 |    |    |-- Kolkata Knight Riders: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- Royal Challengers Bangalore: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |-- registry: struct (nullable = true)
 |    |    |-- people: struct (nullable = true)
 |    |    |    |-- AA Noffke: string (nullable = true)
 |    |    |    |-- AB Agarkar: string (nullable = true)
 |    |    |    |-- AB Dinda: string (nullable = true)
 |    |    |    |-- AM Saheba: string (nullable = true)
 |    |    |    |-- Asad Rauf: string (nullable = true)
 |    |    |    |-- B Akhil: string (nullable = true)
 |    |    |    |-- BB McCullum: string (nullable = true)
 |    |    |    |-- CL White: string (nullable = true)
 |    |    |    |-- DJ Hussey: string (nullable = true)
 |    |    |    |-- I Sharma: string (nullable = true)
 |    |    |    |-- J Srinath: string (nullable = true)
 |    |    |    |-- JH Kallis: string (nullable = true)
 |    |    |    |-- LR Shukla: string (nullable = true)
 |    |    |    |-- M Kartik: string (nullable = true)
 |    |    |    |-- MV Boucher: string (nullable = true)
 |    |    |    |-- Mohammad Hafeez: string (nullable = true)
 |    |    |    |-- P Kumar: string (nullable = true)
 |    |    |    |-- R Dravid: string (nullable = true)
 |    |    |    |-- RE Koertzen: string (nullable = true)
 |    |    |    |-- RT Ponting: string (nullable = true)
 |    |    |    |-- SB Joshi: string (nullable = true)
 |    |    |    |-- SC Ganguly: string (nullable = true)
 |    |    |    |-- V Kohli: string (nullable = true)
 |    |    |    |-- VN Kulkarni: string (nullable = true)
 |    |    |    |-- W Jaffer: string (nullable = true)
 |    |    |    |-- WP Saha: string (nullable = true)
 |    |    |    |-- Z Khan: string (nullable = true)
 |    |-- season: string (nullable = true)
 |    |-- team_type: string (nullable = true)
 |    |-- teams: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- toss: struct (nullable = true)
 |    |    |-- decision: string (nullable = true)
 |    |    |-- winner: string (nullable = true)
 |    |-- venue: string (nullable = true)
 |-- innings: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- overs: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- deliveries: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- batter: string (nullable = true)
 |    |    |    |    |    |    |-- bowler: string (nullable = true)
 |    |    |    |    |    |    |-- extras: struct (nullable = true)
 |    |    |    |    |    |    |    |-- byes: long (nullable = true)
 |    |    |    |    |    |    |    |-- legbyes: long (nullable = true)
 |    |    |    |    |    |    |    |-- wides: long (nullable = true)
 |    |    |    |    |    |    |-- non_striker: string (nullable = true)
 |    |    |    |    |    |    |-- runs: struct (nullable = true)
 |    |    |    |    |    |    |    |-- batter: long (nullable = true)
 |    |    |    |    |    |    |    |-- extras: long (nullable = true)
 |    |    |    |    |    |    |    |-- total: long (nullable = true)
 |    |    |    |    |    |    |-- wickets: array (nullable = true)
 |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |    |    |-- fielders: array (nullable = true)
 |    |    |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |    |    |    |-- kind: string (nullable = true)
 |    |    |    |    |    |    |    |    |-- player_out: string (nullable = true)
 |    |    |    |    |-- over: long (nullable = true)
 |    |    |-- powerplays: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- from: double (nullable = true)
 |    |    |    |    |-- to: double (nullable = true)
 |    |    |    |    |-- type: string (nullable = true)
 |    |    |-- target: struct (nullable = true)
 |    |    |    |-- overs: long (nullable = true)
 |    |    |    |-- runs: long (nullable = true)
 |    |    |-- team: string (nullable = true)
 |-- meta: struct (nullable = true)
 |    |-- created: string (nullable = true)
 |    |-- data_version: string (nullable = true)
 |    |-- revision: long (nullable = true)

>>> var2 = var.select("innings")
>>> var3 = var2.select("overs")
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/dataframe.py", line 1669, in select
    jdf = self._jdf.select(self._jcols(*cols))
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: cannot resolve '`overs`' given input columns: [innings];
'Project ['overs]
+- Project [innings#7]
   +- Relation[info#6,innings#7,meta#8] json

>>> var3 = var2.select("innings.overs")
>>> var3.printSchema()
root
 |-- overs: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- deliveries: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- batter: string (nullable = true)
 |    |    |    |    |    |-- bowler: string (nullable = true)
 |    |    |    |    |    |-- extras: struct (nullable = true)
 |    |    |    |    |    |    |-- byes: long (nullable = true)
 |    |    |    |    |    |    |-- legbyes: long (nullable = true)
 |    |    |    |    |    |    |-- wides: long (nullable = true)
 |    |    |    |    |    |-- non_striker: string (nullable = true)
 |    |    |    |    |    |-- runs: struct (nullable = true)
 |    |    |    |    |    |    |-- batter: long (nullable = true)
 |    |    |    |    |    |    |-- extras: long (nullable = true)
 |    |    |    |    |    |    |-- total: long (nullable = true)
 |    |    |    |    |    |-- wickets: array (nullable = true)
 |    |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |    |-- fielders: array (nullable = true)
 |    |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |    |    |-- kind: string (nullable = true)
 |    |    |    |    |    |    |    |-- player_out: string (nullable = true)
 |    |    |    |-- over: long (nullable = true)

>>> var4 = var2.select(explode("innings.overs"))
>>> var4.printSchema()
root
 |-- col: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- deliveries: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- batter: string (nullable = true)
 |    |    |    |    |-- bowler: string (nullable = true)
 |    |    |    |    |-- extras: struct (nullable = true)
 |    |    |    |    |    |-- byes: long (nullable = true)
 |    |    |    |    |    |-- legbyes: long (nullable = true)
 |    |    |    |    |    |-- wides: long (nullable = true)
 |    |    |    |    |-- non_striker: string (nullable = true)
 |    |    |    |    |-- runs: struct (nullable = true)
 |    |    |    |    |    |-- batter: long (nullable = true)
 |    |    |    |    |    |-- extras: long (nullable = true)
 |    |    |    |    |    |-- total: long (nullable = true)
 |    |    |    |    |-- wickets: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- fielders: array (nullable = true)
 |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |    |-- kind: string (nullable = true)
 |    |    |    |    |    |    |-- player_out: string (nullable = true)
 |    |    |-- over: long (nullable = true)

>>> var5=var4.select("deliveries")
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/dataframe.py", line 1669, in select
    jdf = self._jdf.select(self._jcols(*cols))
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: cannot resolve '`deliveries`' given input columns: [col];
'Project ['deliveries]
+- Project [col#16]
   +- Generate explode(innings#7.overs), false, [col#16]
      +- Project [innings#7]
         +- Relation[info#6,innings#7,meta#8] json

>>> var5=var4.select("col.deliveries")
>>> var5.printSchema()
root
 |-- deliveries: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- batter: string (nullable = true)
 |    |    |    |-- bowler: string (nullable = true)
 |    |    |    |-- extras: struct (nullable = true)
 |    |    |    |    |-- byes: long (nullable = true)
 |    |    |    |    |-- legbyes: long (nullable = true)
 |    |    |    |    |-- wides: long (nullable = true)
 |    |    |    |-- non_striker: string (nullable = true)
 |    |    |    |-- runs: struct (nullable = true)
 |    |    |    |    |-- batter: long (nullable = true)
 |    |    |    |    |-- extras: long (nullable = true)
 |    |    |    |    |-- total: long (nullable = true)
 |    |    |    |-- wickets: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- fielders: array (nullable = true)
 |    |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |-- kind: string (nullable = true)
 |    |    |    |    |    |-- player_out: string (nullable = true)

>>> var6 =var5.select("batter")
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/dataframe.py", line 1669, in select
    jdf = self._jdf.select(self._jcols(*cols))
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: cannot resolve '`batter`' given input columns: [deliveries];
'Project ['batter]
+- Project [col#16.deliveries AS deliveries#18]
   +- Project [col#16]
      +- Generate explode(innings#7.overs), false, [col#16]
         +- Project [innings#7]
            +- Relation[info#6,innings#7,meta#8] json

>>> var5=var4.select("deliveries.batter")
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/dataframe.py", line 1669, in select
    jdf = self._jdf.select(self._jcols(*cols))
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: cannot resolve '`deliveries.batter`' given input columns: [col];
'Project ['deliveries.batter]
+- Project [col#16]
   +- Generate explode(innings#7.overs), false, [col#16]
      +- Project [innings#7]
         +- Relation[info#6,innings#7,meta#8] json

>>> var5=var4.select(explode("col.deliveries"))
>>> var5.printSchema()
root
 |-- col: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- batter: string (nullable = true)
 |    |    |-- bowler: string (nullable = true)
 |    |    |-- extras: struct (nullable = true)
 |    |    |    |-- byes: long (nullable = true)
 |    |    |    |-- legbyes: long (nullable = true)
 |    |    |    |-- wides: long (nullable = true)
 |    |    |-- non_striker: string (nullable = true)
 |    |    |-- runs: struct (nullable = true)
 |    |    |    |-- batter: long (nullable = true)
 |    |    |    |-- extras: long (nullable = true)
 |    |    |    |-- total: long (nullable = true)
 |    |    |-- wickets: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- fielders: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |-- kind: string (nullable = true)
 |    |    |    |    |-- player_out: string (nullable = true)

>>> var6=var4.select("col.batter"))
  File "<stdin>", line 1
    var6=var4.select("col.batter"))
                                  ^
SyntaxError: unmatched ')'
>>> var6=var4.select("col.batter")
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/dataframe.py", line 1669, in select
    jdf = self._jdf.select(self._jcols(*cols))
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: No such struct field batter in deliveries, over
>>> var4.show
<bound method DataFrame.show of DataFrame[col: array<struct<deliveries:array<struct<batter:string,bowler:string,extras:struct<byes:bigint,legbyes:bigint,wides:bigint>,non_striker:string,runs:struct<batter:bigint,extras:bigint,total:bigint>,wickets:array<struct<fielders:array<struct<name:string>>,kind:string,player_out:string>>>>,over:bigint>>]>
>>> var4.show()
[Stage 2:>                                                          (0 + 1) / 1                                                                               +--------------------+
|                 col|
+--------------------+
|[{[{SC Ganguly, P...|
|[{[{R Dravid, AB ...|
+--------------------+

>>> var6=var4.select("col.batter")
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/dataframe.py", line 1669, in select
    jdf = self._jdf.select(self._jcols(*cols))
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: No such struct field batter in deliveries, over
>>> var6=var4.select("deliveries.over.batter")
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/dataframe.py", line 1669, in select
    jdf = self._jdf.select(self._jcols(*cols))
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
  File "/opt/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: cannot resolve '`deliveries.over.batter`' given input columns: [col];
'Project ['deliveries.over.batter]
+- Project [col#16]
   +- Generate explode(innings#7.overs), false, [col#16]
      +- Project [innings#7]
         +- Relation[info#6,innings#7,meta#8] json

>>> var5.printSchema()
root
 |-- col: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- batter: string (nullable = true)
 |    |    |-- bowler: string (nullable = true)
 |    |    |-- extras: struct (nullable = true)
 |    |    |    |-- byes: long (nullable = true)
 |    |    |    |-- legbyes: long (nullable = true)
 |    |    |    |-- wides: long (nullable = true)
 |    |    |-- non_striker: string (nullable = true)
 |    |    |-- runs: struct (nullable = true)
 |    |    |    |-- batter: long (nullable = true)
 |    |    |    |-- extras: long (nullable = true)
 |    |    |    |-- total: long (nullable = true)
 |    |    |-- wickets: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- fielders: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |-- kind: string (nullable = true)
 |    |    |    |    |-- player_out: string (nullable = true)

>>> var5.select("col.batter").show()
+--------------------+
|              batter|
+--------------------+
|[SC Ganguly, BB M...|
|[BB McCullum, BB ...|
|[SC Ganguly, SC G...|
|[BB McCullum, BB ...|
|[SC Ganguly, SC G...|
|[BB McCullum, SC ...|
|[BB McCullum, RT ...|
|[BB McCullum, BB ...|
|[BB McCullum, BB ...|
|[RT Ponting, BB M...|
|[BB McCullum, RT ...|
|[BB McCullum, BB ...|
|[RT Ponting, BB M...|
|[BB McCullum, DJ ...|
|[DJ Hussey, DJ Hu...|
|[DJ Hussey, DJ Hu...|
|[BB McCullum, DJ ...|
|[DJ Hussey, BB Mc...|
|[BB McCullum, BB ...|
|[BB McCullum, BB ...|
+--------------------+
only showing top 20 rows

>>> dataSet = var5.select("col.batter","col.bowler",col("col.runs.batter").alias("runs"))
>>> dataSet.show()
+--------------------+--------------------+--------------------+
|              batter|              bowler|                runs|
+--------------------+--------------------+--------------------+
|[SC Ganguly, BB M...|[P Kumar, P Kumar...|[0, 0, 0, 0, 0, 0...|
|[BB McCullum, BB ...|[Z Khan, Z Khan, ...|  [0, 4, 4, 6, 4, 0]|
|[SC Ganguly, SC G...|[P Kumar, P Kumar...|  [0, 0, 0, 4, 1, 0]|
|[BB McCullum, BB ...|[AA Noffke, AA No...|[0, 6, 0, 4, 0, 1...|
|[SC Ganguly, SC G...|[P Kumar, P Kumar...|  [4, 1, 4, 0, 1, 0]|
|[BB McCullum, SC ...|[Z Khan, Z Khan, ...|  [1, 0, 0, 0, 0, 0]|
|[BB McCullum, RT ...|[AA Noffke, AA No...|  [1, 1, 1, 2, 1, 1]|
|[BB McCullum, BB ...|[Z Khan, Z Khan, ...|  [0, 1, 1, 1, 1, 1]|
|[BB McCullum, BB ...|[JH Kallis, JH Ka...|  [0, 0, 0, 1, 1, 2]|
|[RT Ponting, BB M...|[SB Joshi, SB Jos...|  [1, 1, 1, 0, 6, 1]|
|[BB McCullum, RT ...|[JH Kallis, JH Ka...|  [1, 4, 0, 6, 0, 0]|
|[BB McCullum, BB ...|[SB Joshi, SB Jos...|  [0, 6, 2, 1, 0, 1]|
|[RT Ponting, BB M...|[JH Kallis, JH Ka...|[0, 4, 0, 2, 0, 4...|
|[BB McCullum, DJ ...|[SB Joshi, SB Jos...|  [1, 0, 1, 1, 1, 2]|
|[DJ Hussey, DJ Hu...|[CL White, CL Whi...|[4, 1, 6, 4, 0, 1...|
|[DJ Hussey, DJ Hu...|[AA Noffke, AA No...|  [0, 1, 2, 0, 1, 0]|
|[BB McCullum, DJ ...|[Z Khan, Z Khan, ...|  [1, 2, 1, 6, 2, 2]|
|[DJ Hussey, BB Mc...|[AA Noffke, AA No...|  [0, 1, 0, 1, 4, 1]|
|[BB McCullum, BB ...|[JH Kallis, JH Ka...|  [6, 0, 6, 4, 1, 4]|
|[BB McCullum, BB ...|[P Kumar, P Kumar...|  [6, 6, 2, 0, 2, 6]|
+--------------------+--------------------+--------------------+
only showing top 20 rows

>>> dataSet.

