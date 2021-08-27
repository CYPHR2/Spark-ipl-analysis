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

























summary()|SC Ganguly|P Kumar|1   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|1   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|1   |
|SC Ganguly|P Kumar|2   |
|SC Ganguly|P Kumar|1   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|1   |
|SC Ganguly|P Kumar|1   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|6   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|1   |
|SC Ganguly|P Kumar|1   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|1   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|4   |
|SC Ganguly|P Kumar|1   |
|SC Ganguly|P Kumar|0   |
|SC Ganguly|P Kumar|1   |
|SC Ganguly|P Kumar|2   |
|SC Ganguly|P Kumar|0   |
+----------+-------+----+
only showing top 400 rows

>>> 
>>> bowlers_join.summary().show()
+-------+---------+
|summary|   bowler|
+-------+---------+
|  count|      225|
|   mean|     null|
| stddev|     null|
|    min|AA Noffke|
|    25%|     null|
|    50%|     null|
|    75%|     null|
|    max|   Z Khan|
+-------+---------+

>>> batsmans_join.summary().show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'batsmans_join' is not defined
>>> batsman_join.summary().show()
+-------+---------+
|summary|  batsman|
+-------+---------+
|  count|      225|
|   mean|     null|
| stddev|     null|
|    min|AA Noffke|
|    25%|     null|
|    50%|     null|
|    75%|     null|
|    max|   Z Khan|
+-------+---------+

>>> empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner")
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'empDF' is not defined
>>> join_var =  batsman_join.join(batsman_join,batsman_join.runs == bowlers_join.runs,"inner")
>>> join_var.show()
+--------------------+-----------+--------------------+--------------------+-----------+--------------------+
|              bowler|    batsman|                runs|              bowler|    batsman|                runs|
+--------------------+-----------+--------------------+--------------------+-----------+--------------------+
|[P Kumar, P Kumar...| SC Ganguly|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...| SC Ganguly|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...| SC Ganguly|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...| SC Ganguly|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...| SC Ganguly|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...| SC Ganguly|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...| SC Ganguly|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...| SC Ganguly|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...| SC Ganguly|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|[P Kumar, P Kumar...|BB McCullum|[0, 0, 0, 0, 0, 0...|
+--------------------+-----------+--------------------+--------------------+-----------+--------------------+
only showing top 20 rows

>>> join_var.summary().show()
[Stage 53:>                                                         (0 + 1) / 1                                                                               +-------+---------+---------+
|summary|  batsman|  batsman|
+-------+---------+---------+
|  count|     1437|     1437|
|   mean|     null|     null|
| stddev|     null|     null|
|    min|AA Noffke|AA Noffke|
|    25%|     null|     null|
|    50%|     null|     null|
|    75%|     null|     null|
|    max|   Z Khan|   Z Khan|
+-------+---------+---------+

>>> join_var =  batsman_join.withColumn("newCol",col(bat.runs))
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/dataframe.py", line 1643, in __getattr__
    raise AttributeError(
AttributeError: 'DataFrame' object has no attribute 'runs'
>>> join_var =  batsman_join.withColumn("newCol",col(bat.run))
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/dataframe.py", line 1643, in __getattr__
    raise AttributeError(
AttributeError: 'DataFrame' object has no attribute 'run'
>>> bat.show()
+-----------+
|    batsman|
+-----------+
| SC Ganguly|
|BB McCullum|
|BB McCullum|
|BB McCullum|
|BB McCullum|
|BB McCullum|
|BB McCullum|
|BB McCullum|
|BB McCullum|
|BB McCullum|
|BB McCullum|
|BB McCullum|
|BB McCullum|
| SC Ganguly|
| SC Ganguly|
| SC Ganguly|
|BB McCullum|
|BB McCullum|
| SC Ganguly|
|BB McCullum|
+-----------+
only showing top 20 rows

>>> run.show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'run' is not defined
>>> runs.show()
+----+
|runs|
+----+
|   0|
|   0|
|   0|
|   0|
|   0|
|   0|
|   0|
|   0|
|   4|
|   4|
|   6|
|   4|
|   0|
|   0|
|   0|
|   0|
|   4|
|   1|
|   0|
|   0|
+----+
only showing top 20 rows

>>> join_var =  batsman_join.withColumn("newCol",col(runs.runs))
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/functions.py", line 106, in col
    return _invoke_function("col", col)
  File "/opt/spark/python/pyspark/sql/functions.py", line 58, in _invoke_function
    return Column(jf(*args))
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1296, in __call__
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1260, in _build_args
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1247, in _get_args
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_collections.py", line 510, in convert
  File "/opt/spark/python/pyspark/sql/column.py", line 460, in __iter__
    raise TypeError("Column is not iterable")
TypeError: Column is not iterable
>>> join_var =  batsman_join.withColumn("newCol",col(batsman.runs))
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'batsman' is not defined
>>> join_var =  batsman_join.withColumn("newCol",col(batsman_join.runs))
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/sql/functions.py", line 106, in col
    return _invoke_function("col", col)
  File "/opt/spark/python/pyspark/sql/functions.py", line 58, in _invoke_function
    return Column(jf(*args))
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1296, in __call__
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1260, in _build_args
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1247, in _get_args
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_collections.py", line 510, in convert
  File "/opt/spark/python/pyspark/sql/column.py", line 460, in __iter__
    raise TypeError("Column is not iterable")
TypeError: Column is not iterable
>>> 
>>> 
>>> 
>>> 
>>> from pyspark.sql.functions import monotonically_increasing_id, row_number
>>> from pyspark.sql.window import Window
>>> 
>>> minDf = sc.parallelize(bat)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/spark/python/pyspark/context.py", line 566, in parallelize
    jrdd = self._serialize_to_jvm(c, serializer, reader_func, createRDDServer)
  File "/opt/spark/python/pyspark/context.py", line 603, in _serialize_to_jvm
    serializer.dump_stream(data, tempFile)
  File "/opt/spark/python/pyspark/serializers.py", line 211, in dump_stream
    self.serializer.dump_stream(self._batched(iterator), stream)
  File "/opt/spark/python/pyspark/serializers.py", line 133, in dump_stream
    self._write_with_length(obj, stream)
  File "/opt/spark/python/pyspark/serializers.py", line 143, in _write_with_length
    serialized = self.dumps(obj)
  File "/opt/spark/python/pyspark/serializers.py", line 427, in dumps
    return pickle.dumps(obj, pickle_protocol)
TypeError: cannot pickle '_thread.RLock' object
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
>>> bat_rdd = bat.rdd()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: 'RDD' object is not callable
>>> bat_rdd = bat.rdd
>>> print(bat_rdd.collect())
[Row(batsman='SC Ganguly'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='SC Ganguly'), Row(batsman='SC Ganguly'), Row(batsman='SC Ganguly'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='SC Ganguly'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='SC Ganguly'), Row(batsman='SC Ganguly'), Row(batsman='SC Ganguly'), Row(batsman='BB McCullum'), Row(batsman='SC Ganguly'), Row(batsman='SC Ganguly'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='SC Ganguly'), Row(batsman='BB McCullum'), Row(batsman='SC Ganguly'), Row(batsman='RT Ponting'), Row(batsman='RT Ponting'), Row(batsman='RT Ponting'), Row(batsman='RT Ponting'), Row(batsman='BB McCullum'), Row(batsman='RT Ponting'), Row(batsman='BB McCullum'), Row(batsman='RT Ponting'), Row(batsman='RT Ponting'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='RT Ponting'), Row(batsman='BB McCullum'), Row(batsman='RT Ponting'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='RT Ponting'), Row(batsman='BB McCullum'), Row(batsman='RT Ponting'), Row(batsman='BB McCullum'), Row(batsman='RT Ponting'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='RT Ponting'), Row(batsman='RT Ponting'), Row(batsman='RT Ponting'), Row(batsman='RT Ponting'), Row(batsman='RT Ponting'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='RT Ponting'), Row(batsman='RT Ponting'), Row(batsman='RT Ponting'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='DJ Hussey'), Row(batsman='DJ Hussey'), Row(batsman='BB McCullum'), Row(batsman='DJ Hussey'), Row(batsman='BB McCullum'), Row(batsman='DJ Hussey'), Row(batsman='DJ Hussey'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='DJ Hussey'), Row(batsman='BB McCullum'), Row(batsman='DJ Hussey'), Row(batsman='DJ Hussey'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='DJ Hussey'), Row(batsman='BB McCullum'), Row(batsman='DJ Hussey'), Row(batsman='DJ Hussey'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='DJ Hussey'), Row(batsman='BB McCullum'), Row(batsman='Mohammad Hafeez'), Row(batsman='Mohammad Hafeez'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='Mohammad Hafeez'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='BB McCullum'), Row(batsman='R Dravid'), Row(batsman='W Jaffer'), Row(batsman='W Jaffer'), Row(batsman='W Jaffer'), Row(batsman='R Dravid'), Row(batsman='W Jaffer'), Row(batsman='W Jaffer'), Row(batsman='R Dravid'), Row(batsman='V Kohli'), Row(batsman='V Kohli'), Row(batsman='V Kohli'), Row(batsman='W Jaffer'), Row(batsman='W Jaffer'), Row(batsman='V Kohli'), Row(batsman='V Kohli'), Row(batsman='JH Kallis'), Row(batsman='W Jaffer'), Row(batsman='W Jaffer'), Row(batsman='W Jaffer'), Row(batsman='W Jaffer'), Row(batsman='W Jaffer'), Row(batsman='W Jaffer'), Row(batsman='JH Kallis'), Row(batsman='JH Kallis'), Row(batsman='W Jaffer'), Row(batsman='W Jaffer'), Row(batsman='JH Kallis'), Row(batsman='W Jaffer'), Row(batsman='JH Kallis'), Row(batsman='JH Kallis'), Row(batsman='JH Kallis'), Row(batsman='CL White'), Row(batsman='W Jaffer'), Row(batsman='W Jaffer'), Row(batsman='MV Boucher'), Row(batsman='MV Boucher'), Row(batsman='CL White'), Row(batsman='MV Boucher'), Row(batsman='CL White'), Row(batsman='CL White'), Row(batsman='CL White'), Row(batsman='MV Boucher'), Row(batsman='MV Boucher'), Row(batsman='MV Boucher'), Row(batsman='MV Boucher'), Row(batsman='MV Boucher'), Row(batsman='CL White'), Row(batsman='MV Boucher'), Row(batsman='CL White'), Row(batsman='MV Boucher'), Row(batsman='MV Boucher'), Row(batsman='CL White'), Row(batsman='B Akhil'), Row(batsman='B Akhil'), Row(batsman='AA Noffke'), Row(batsman='AA Noffke'), Row(batsman='AA Noffke'), Row(batsman='AA Noffke'), Row(batsman='CL White'), Row(batsman='CL White'), Row(batsman='AA Noffke'), Row(batsman='P Kumar'), Row(batsman='P Kumar'), Row(batsman='P Kumar'), Row(batsman='P Kumar'), Row(batsman='AA Noffke'), Row(batsman='AA Noffke'), Row(batsman='AA Noffke'), Row(batsman='AA Noffke'), Row(batsman='AA Noffke'), Row(batsman='AA Noffke'), Row(batsman='P Kumar'), Row(batsman='P Kumar'), Row(batsman='AA Noffke'), Row(batsman='Z Khan'), Row(batsman='Z Khan'), Row(batsman='Z Khan'), Row(batsman='P Kumar'), Row(batsman='P Kumar'), Row(batsman='P Kumar'), Row(batsman='P Kumar'), Row(batsman='Z Khan'), Row(batsman='P Kumar'), Row(batsman='Z Khan'), Row(batsman='Z Khan'), Row(batsman='Z Khan'), Row(batsman='Z Khan'), Row(batsman='SB Joshi'), Row(batsman='P Kumar'), Row(batsman='P Kumar'), Row(batsman='P Kumar'), Row(batsman='P Kumar'), Row(batsman='SB Joshi'), Row(batsman='SB Joshi'), Row(batsman='SB Joshi'), Row(batsman='SB Joshi'), Row(batsman='P Kumar'), Row(batsman='SB Joshi'), Row(batsman='P Kumar'), Row(batsman='SB Joshi'), Row(batsman='SB Joshi')]
>>> bowler_rdd = bowlers.rdd
>>> print(bowler_rdd.collect())
[Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='SB Joshi'), Row(bowler='CL White'), Row(bowler='CL White'), Row(bowler='CL White'), Row(bowler='CL White'), Row(bowler='CL White'), Row(bowler='CL White'), Row(bowler='CL White'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='Z Khan'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='AA Noffke'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='JH Kallis'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='P Kumar'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Dinda'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='AB Agarkar'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='LR Shukla'), Row(bowler='LR Shukla'), Row(bowler='LR Shukla'), Row(bowler='LR Shukla'), Row(bowler='LR Shukla'), Row(bowler='LR Shukla'), Row(bowler='LR Shukla'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='SC Ganguly'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='I Sharma'), Row(bowler='LR Shukla'), Row(bowler='LR Shukla')]
>>> bat_rdd=bat_rdd.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
AttributeError: 'RDD' object has no attribute 'withColumn'
>>> bat_rdd=bat.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
>>> bowler_rdd=bowlers.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
>>> total = bowler_rdd.join(bat_rdd, on=["row_index"]).drop("row_index")
>>> total.show()
21/08/27 13:14:44 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/08/27 13:14:44 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
+---------+-----------+
|   bowler|    batsman|
+---------+-----------+
|  P Kumar| SC Ganguly|
|  P Kumar|BB McCullum|
|  P Kumar|BB McCullum|
|  P Kumar|BB McCullum|
|  P Kumar|BB McCullum|
|  P Kumar|BB McCullum|
|  P Kumar|BB McCullum|
|   Z Khan|BB McCullum|
|   Z Khan|BB McCullum|
|   Z Khan|BB McCullum|
|   Z Khan|BB McCullum|
|   Z Khan|BB McCullum|
|   Z Khan|BB McCullum|
|  P Kumar| SC Ganguly|
|  P Kumar| SC Ganguly|
|  P Kumar| SC Ganguly|
|  P Kumar|BB McCullum|
|  P Kumar|BB McCullum|
|  P Kumar| SC Ganguly|
|AA Noffke|BB McCullum|
+---------+-----------+
only showing top 20 rows

>>> total.summary().show()
21/08/27 13:15:03 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/08/27 13:15:03 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
+-------+---------+---------+
|summary|   bowler|  batsman|
+-------+---------+---------+
|  count|      225|      225|
|   mean|     null|     null|
| stddev|     null|     null|
|    min|AA Noffke|AA Noffke|
|    25%|     null|     null|
|    50%|     null|     null|
|    75%|     null|     null|
|    max|   Z Khan|   Z Khan|
+-------+---------+---------+

>>> runs_rdd=runs.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
>>> total=total.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
>>> total = total.join(runs_rdd, on=["row_index"]).drop("row_index")
>>> total.summary().show()
21/08/27 13:17:51 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/08/27 13:17:51 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/08/27 13:17:51 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/08/27 13:17:51 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
[Stage 72:>                                                         (0 + 1) / 1                                                                               +-------+---------+---------+-----------------+
|summary|   bowler|  batsman|             runs|
+-------+---------+---------+-----------------+
|  count|      225|      225|              225|
|   mean|     null|     null|1.191111111111111|
| stddev|     null|     null|1.771276555177264|
|    min|AA Noffke|AA Noffke|                0|
|    25%|     null|     null|                0|
|    50%|     null|     null|                1|
|    75%|     null|     null|                1|
|    max|   Z Khan|   Z Khan|                6|
+-------+---------+---------+-----------------+

>>> total.show(400,False)
21/08/27 13:18:13 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/08/27 13:18:13 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/08/27 13:18:13 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/08/27 13:18:13 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
+----------+---------------+----+
|bowler    |batsman        |runs|
+----------+---------------+----+
|P Kumar   |SC Ganguly     |0   |
|P Kumar   |BB McCullum    |0   |
|P Kumar   |BB McCullum    |0   |
|P Kumar   |BB McCullum    |0   |
|P Kumar   |BB McCullum    |0   |
|P Kumar   |BB McCullum    |0   |
|P Kumar   |BB McCullum    |0   |
|Z Khan    |BB McCullum    |0   |
|Z Khan    |BB McCullum    |4   |
|Z Khan    |BB McCullum    |4   |
|Z Khan    |BB McCullum    |6   |
|Z Khan    |BB McCullum    |4   |
|Z Khan    |BB McCullum    |0   |
|P Kumar   |SC Ganguly     |0   |
|P Kumar   |SC Ganguly     |0   |
|P Kumar   |SC Ganguly     |0   |
|P Kumar   |BB McCullum    |4   |
|P Kumar   |BB McCullum    |1   |
|P Kumar   |SC Ganguly     |0   |
|AA Noffke |BB McCullum    |0   |
|AA Noffke |BB McCullum    |6   |
|AA Noffke |BB McCullum    |0   |
|AA Noffke |SC Ganguly     |4   |
|AA Noffke |SC Ganguly     |0   |
|AA Noffke |SC Ganguly     |1   |
|AA Noffke |BB McCullum    |6   |
|P Kumar   |SC Ganguly     |4   |
|P Kumar   |SC Ganguly     |1   |
|P Kumar   |BB McCullum    |4   |
|P Kumar   |BB McCullum    |0   |
|P Kumar   |BB McCullum    |1   |
|P Kumar   |SC Ganguly     |0   |
|Z Khan    |BB McCullum    |1   |
|Z Khan    |SC Ganguly     |0   |
|Z Khan    |RT Ponting     |0   |
|Z Khan    |RT Ponting     |0   |
|Z Khan    |RT Ponting     |0   |
|Z Khan    |RT Ponting     |0   |
|AA Noffke |BB McCullum    |1   |
|AA Noffke |RT Ponting     |1   |
|AA Noffke |BB McCullum    |1   |
|AA Noffke |RT Ponting     |2   |
|AA Noffke |RT Ponting     |1   |
|AA Noffke |BB McCullum    |1   |
|Z Khan    |BB McCullum    |0   |
|Z Khan    |BB McCullum    |1   |
|Z Khan    |RT Ponting     |1   |
|Z Khan    |BB McCullum    |1   |
|Z Khan    |RT Ponting     |1   |
|Z Khan    |BB McCullum    |1   |
|JH Kallis |BB McCullum    |0   |
|JH Kallis |BB McCullum    |0   |
|JH Kallis |BB McCullum    |0   |
|JH Kallis |BB McCullum    |1   |
|JH Kallis |RT Ponting     |1   |
|JH Kallis |BB McCullum    |2   |
|SB Joshi  |RT Ponting     |1   |
|SB Joshi  |BB McCullum    |1   |
|SB Joshi  |RT Ponting     |1   |
|SB Joshi  |BB McCullum    |0   |
|SB Joshi  |BB McCullum    |6   |
|SB Joshi  |BB McCullum    |1   |
|JH Kallis |BB McCullum    |1   |
|JH Kallis |RT Ponting     |4   |
|JH Kallis |RT Ponting     |0   |
|JH Kallis |RT Ponting     |6   |
|JH Kallis |RT Ponting     |0   |
|JH Kallis |RT Ponting     |0   |
|SB Joshi  |BB McCullum    |0   |
|SB Joshi  |BB McCullum    |6   |
|SB Joshi  |BB McCullum    |2   |
|SB Joshi  |BB McCullum    |1   |
|SB Joshi  |RT Ponting     |0   |
|SB Joshi  |RT Ponting     |1   |
|JH Kallis |RT Ponting     |0   |
|JH Kallis |BB McCullum    |4   |
|JH Kallis |BB McCullum    |0   |
|JH Kallis |BB McCullum    |2   |
|JH Kallis |BB McCullum    |0   |
|JH Kallis |BB McCullum    |4   |
|JH Kallis |BB McCullum    |1   |
|SB Joshi  |BB McCullum    |1   |
|SB Joshi  |DJ Hussey      |0   |
|SB Joshi  |DJ Hussey      |1   |
|SB Joshi  |BB McCullum    |1   |
|SB Joshi  |DJ Hussey      |1   |
|SB Joshi  |BB McCullum    |2   |
|CL White  |DJ Hussey      |4   |
|CL White  |DJ Hussey      |1   |
|CL White  |BB McCullum    |6   |
|CL White  |BB McCullum    |4   |
|CL White  |BB McCullum    |0   |
|CL White  |DJ Hussey      |1   |
|CL White  |BB McCullum    |6   |
|AA Noffke |DJ Hussey      |0   |
|AA Noffke |DJ Hussey      |1   |
|AA Noffke |BB McCullum    |2   |
|AA Noffke |BB McCullum    |0   |
|AA Noffke |BB McCullum    |1   |
|AA Noffke |DJ Hussey      |0   |
|Z Khan    |BB McCullum    |1   |
|Z Khan    |DJ Hussey      |2   |
|Z Khan    |DJ Hussey      |1   |
|Z Khan    |BB McCullum    |6   |
|Z Khan    |BB McCullum    |2   |
|Z Khan    |BB McCullum    |2   |
|AA Noffke |DJ Hussey      |0   |
|AA Noffke |BB McCullum    |1   |
|AA Noffke |Mohammad Hafeez|0   |
|AA Noffke |Mohammad Hafeez|1   |
|AA Noffke |BB McCullum    |4   |
|AA Noffke |BB McCullum    |1   |
|JH Kallis |BB McCullum    |6   |
|JH Kallis |BB McCullum    |0   |
|JH Kallis |BB McCullum    |6   |
|JH Kallis |BB McCullum    |4   |
|JH Kallis |BB McCullum    |1   |
|JH Kallis |Mohammad Hafeez|4   |
|P Kumar   |BB McCullum    |6   |
|P Kumar   |BB McCullum    |6   |
|P Kumar   |BB McCullum    |2   |
|P Kumar   |BB McCullum    |0   |
|P Kumar   |BB McCullum    |2   |
|P Kumar   |BB McCullum    |6   |
|AB Dinda  |R Dravid       |1   |
|AB Dinda  |W Jaffer       |0   |
|AB Dinda  |W Jaffer       |0   |
|AB Dinda  |W Jaffer       |1   |
|AB Dinda  |R Dravid       |1   |
|AB Dinda  |W Jaffer       |0   |
|AB Dinda  |W Jaffer       |0   |
|I Sharma  |R Dravid       |0   |
|I Sharma  |V Kohli        |0   |
|I Sharma  |V Kohli        |0   |
|I Sharma  |V Kohli        |1   |
|I Sharma  |W Jaffer       |0   |
|I Sharma  |W Jaffer       |0   |
|AB Dinda  |V Kohli        |0   |
|AB Dinda  |V Kohli        |0   |
|AB Dinda  |JH Kallis      |1   |
|AB Dinda  |W Jaffer       |0   |
|AB Dinda  |W Jaffer       |0   |
|AB Dinda  |W Jaffer       |0   |
|AB Dinda  |W Jaffer       |1   |
|I Sharma  |W Jaffer       |2   |
|I Sharma  |W Jaffer       |1   |
|I Sharma  |JH Kallis      |0   |
|I Sharma  |JH Kallis      |0   |
|I Sharma  |W Jaffer       |0   |
|I Sharma  |W Jaffer       |0   |
|AB Agarkar|JH Kallis      |1   |
|AB Agarkar|W Jaffer       |1   |
|AB Agarkar|JH Kallis      |0   |
|AB Agarkar|JH Kallis      |6   |
|AB Agarkar|JH Kallis      |0   |
|AB Agarkar|CL White       |0   |
|AB Dinda  |W Jaffer       |0   |
|AB Dinda  |W Jaffer       |0   |
|AB Dinda  |MV Boucher     |0   |
|AB Dinda  |MV Boucher     |1   |
|AB Dinda  |CL White       |1   |
|AB Dinda  |MV Boucher     |0   |
|AB Agarkar|CL White       |0   |
|AB Agarkar|CL White       |0   |
|AB Agarkar|CL White       |1   |
|AB Agarkar|MV Boucher     |0   |
|AB Agarkar|MV Boucher     |0   |
|AB Agarkar|MV Boucher     |0   |
|AB Agarkar|MV Boucher     |0   |
|AB Agarkar|MV Boucher     |4   |
|SC Ganguly|CL White       |1   |
|SC Ganguly|MV Boucher     |0   |
|SC Ganguly|CL White       |1   |
|SC Ganguly|MV Boucher     |2   |
|SC Ganguly|MV Boucher     |0   |
|SC Ganguly|CL White       |0   |
|AB Agarkar|B Akhil        |0   |
|AB Agarkar|B Akhil        |0   |
|AB Agarkar|AA Noffke      |0   |
|AB Agarkar|AA Noffke      |0   |
|AB Agarkar|AA Noffke      |0   |
|AB Agarkar|AA Noffke      |1   |
|AB Agarkar|CL White       |2   |
|AB Agarkar|CL White       |0   |
|SC Ganguly|AA Noffke      |1   |
|SC Ganguly|P Kumar        |0   |
|SC Ganguly|P Kumar        |0   |
|SC Ganguly|P Kumar        |0   |
|SC Ganguly|P Kumar        |0   |
|SC Ganguly|AA Noffke      |4   |
|SC Ganguly|AA Noffke      |1   |
|AB Agarkar|AA Noffke      |0   |
|AB Agarkar|AA Noffke      |0   |
|AB Agarkar|AA Noffke      |0   |
|AB Agarkar|AA Noffke      |1   |
|AB Agarkar|P Kumar        |4   |
|AB Agarkar|P Kumar        |0   |
|SC Ganguly|AA Noffke      |1   |
|SC Ganguly|Z Khan         |0   |
|SC Ganguly|Z Khan         |0   |
|SC Ganguly|Z Khan         |1   |
|SC Ganguly|P Kumar        |0   |
|SC Ganguly|P Kumar        |1   |
|LR Shukla |P Kumar        |6   |
|LR Shukla |P Kumar        |0   |
|LR Shukla |Z Khan         |1   |
|LR Shukla |P Kumar        |1   |
|LR Shukla |Z Khan         |0   |
|LR Shukla |Z Khan         |0   |
|LR Shukla |Z Khan         |1   |
|SC Ganguly|Z Khan         |0   |
|SC Ganguly|SB Joshi       |1   |
|SC Ganguly|P Kumar        |0   |
|SC Ganguly|P Kumar        |0   |
|SC Ganguly|P Kumar        |0   |
|SC Ganguly|P Kumar        |6   |
|I Sharma  |SB Joshi       |0   |
|I Sharma  |SB Joshi       |0   |
|I Sharma  |SB Joshi       |0   |
|I Sharma  |SB Joshi       |1   |
|I Sharma  |P Kumar        |0   |
|I Sharma  |SB Joshi       |1   |
|I Sharma  |P Kumar        |0   |
|LR Shukla |SB Joshi       |0   |
|LR Shukla |SB Joshi       |0   |
+----------+---------------+----+

>>> statDF =total.groupby(‘batsman’,’bowler’).sum(‘runs’).orderBy(‘batsman’)
  File "<stdin>", line 1
    statDF =total.groupby(‘batsman’,’bowler’).sum(‘runs’).orderBy(‘batsman’)
                                  ^
SyntaxError: invalid character in identifier
>>> statDF =total.groupby("batsman","bowler").sum("runs").orderBy("batsman")
>>> statDf.show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'statDf' is not defined
>>> statDF.show()
21/08/27 13:25:05 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/08/27 13:25:05 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/08/27 13:25:05 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/08/27 13:25:05 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
+-----------+----------+---------+
|    batsman|    bowler|sum(runs)|
+-----------+----------+---------+
|  AA Noffke|AB Agarkar|        2|
|  AA Noffke|SC Ganguly|        7|
|    B Akhil|AB Agarkar|        0|
|BB McCullum|    Z Khan|       33|
|BB McCullum| AA Noffke|       24|
|BB McCullum| JH Kallis|       32|
|BB McCullum|  SB Joshi|       21|
|BB McCullum|  CL White|       16|
|BB McCullum|   P Kumar|       32|
|   CL White|SC Ganguly|        2|
|   CL White|  AB Dinda|        1|
|   CL White|AB Agarkar|        3|
|  DJ Hussey|  CL White|        6|
|  DJ Hussey| AA Noffke|        1|
|  DJ Hussey|  SB Joshi|        2|
|  DJ Hussey|    Z Khan|        3|
|  JH Kallis|  I Sharma|        0|
|  JH Kallis|  AB Dinda|        1|
|  JH Kallis|AB Agarkar|        7|
| MV Boucher|AB Agarkar|        4|
+-----------+----------+---------+
only showing top 20 rows

>>> 
Traceback (most recent call last):
  File "/opt/spark/python/pyspark/context.py", line 285, in signal_handler
    raise KeyboardInterrupt()
KeyboardInterrupt
>>> 


