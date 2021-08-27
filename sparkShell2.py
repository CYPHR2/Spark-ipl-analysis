>>> 
>>> 
>>> 
>>> from pyspark.sql.functions import *

>>> var = spark.read.json("sample.json", multiLine = "true")

>>> var2 = var.select("innings")

>>> var4 = var2.select(explode("innings.overs"))

>>> var5=var4.select(explode("col.deliveries"))


>>> dataSet = var5.select("col.batter","col.bowler",col("col.runs.batter").alias("runs"))





>>> dataSet.show()
[Stage 2:>                                                          (0 + 1) / 1                                                                               +--------------------+--------------------+--------------------+
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

>>> bat = dataSet.select(explode("batter").alias("batsman"))
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

>>> bat.show(400,False)
+---------------+
|batsman        |
+---------------+
|SC Ganguly     |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|SC Ganguly     |
|SC Ganguly     |
|SC Ganguly     |
|BB McCullum    |
|BB McCullum    |
|SC Ganguly     |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|SC Ganguly     |
|SC Ganguly     |
|SC Ganguly     |
|BB McCullum    |
|SC Ganguly     |
|SC Ganguly     |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|SC Ganguly     |
|BB McCullum    |
|SC Ganguly     |
|RT Ponting     |
|RT Ponting     |
|RT Ponting     |
|RT Ponting     |
|BB McCullum    |
|RT Ponting     |
|BB McCullum    |
|RT Ponting     |
|RT Ponting     |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|RT Ponting     |
|BB McCullum    |
|RT Ponting     |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|RT Ponting     |
|BB McCullum    |
|RT Ponting     |
|BB McCullum    |
|RT Ponting     |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|RT Ponting     |
|RT Ponting     |
|RT Ponting     |
|RT Ponting     |
|RT Ponting     |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|RT Ponting     |
|RT Ponting     |
|RT Ponting     |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|DJ Hussey      |
|DJ Hussey      |
|BB McCullum    |
|DJ Hussey      |
|BB McCullum    |
|DJ Hussey      |
|DJ Hussey      |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|DJ Hussey      |
|BB McCullum    |
|DJ Hussey      |
|DJ Hussey      |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|DJ Hussey      |
|BB McCullum    |
|DJ Hussey      |
|DJ Hussey      |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|DJ Hussey      |
|BB McCullum    |
|Mohammad Hafeez|
|Mohammad Hafeez|
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|Mohammad Hafeez|
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|BB McCullum    |
|R Dravid       |
|W Jaffer       |
|W Jaffer       |
|W Jaffer       |
|R Dravid       |
|W Jaffer       |
|W Jaffer       |
|R Dravid       |
|V Kohli        |
|V Kohli        |
|V Kohli        |
|W Jaffer       |
|W Jaffer       |
|V Kohli        |
|V Kohli        |
|JH Kallis      |
|W Jaffer       |
|W Jaffer       |
|W Jaffer       |
|W Jaffer       |
|W Jaffer       |
|W Jaffer       |
|JH Kallis      |
|JH Kallis      |
|W Jaffer       |
|W Jaffer       |
|JH Kallis      |
|W Jaffer       |
|JH Kallis      |
|JH Kallis      |
|JH Kallis      |
|CL White       |
|W Jaffer       |
|W Jaffer       |
|MV Boucher     |
|MV Boucher     |
|CL White       |
|MV Boucher     |
|CL White       |
|CL White       |
|CL White       |
|MV Boucher     |
|MV Boucher     |
|MV Boucher     |
|MV Boucher     |
|MV Boucher     |
|CL White       |
|MV Boucher     |
|CL White       |
|MV Boucher     |
|MV Boucher     |
|CL White       |
|B Akhil        |
|B Akhil        |
|AA Noffke      |
|AA Noffke      |
|AA Noffke      |
|AA Noffke      |
|CL White       |
|CL White       |
|AA Noffke      |
|P Kumar        |
|P Kumar        |
|P Kumar        |
|P Kumar        |
|AA Noffke      |
|AA Noffke      |
|AA Noffke      |
|AA Noffke      |
|AA Noffke      |
|AA Noffke      |
|P Kumar        |
|P Kumar        |
|AA Noffke      |
|Z Khan         |
|Z Khan         |
|Z Khan         |
|P Kumar        |
|P Kumar        |
|P Kumar        |
|P Kumar        |
|Z Khan         |
|P Kumar        |
|Z Khan         |
|Z Khan         |
|Z Khan         |
|Z Khan         |
|SB Joshi       |
|P Kumar        |
|P Kumar        |
|P Kumar        |
|P Kumar        |
|SB Joshi       |
|SB Joshi       |
|SB Joshi       |
|SB Joshi       |
|P Kumar        |
|SB Joshi       |
|P Kumar        |
|SB Joshi       |
|SB Joshi       |
+---------------+

>>> bowlers = dataSet.select(explode("bowler").alias("bowler"))
>>> runs = dataSet.select(explode("runs").alias("runs"))
>>> bowlers.show(400,False)
+----------+
|bowler    |
+----------+
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|SB Joshi  |
|CL White  |
|CL White  |
|CL White  |
|CL White  |
|CL White  |
|CL White  |
|CL White  |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|Z Khan    |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|AA Noffke |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|JH Kallis |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|P Kumar   |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Dinda  |
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|AB Agarkar|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|LR Shukla |
|LR Shukla |
|LR Shukla |
|LR Shukla |
|LR Shukla |
|LR Shukla |
|LR Shukla |
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|SC Ganguly|
|I Sharma  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|I Sharma  |
|LR Shukla |
|LR Shukla |
+----------+

>>> runs.show(400,False)
+----+
|runs|
+----+
|0   |
|0   |
|0   |
|0   |
|0   |
|0   |
|0   |
|0   |
|4   |
|4   |
|6   |
|4   |
|0   |
|0   |
|0   |
|0   |
|4   |
|1   |
|0   |
|0   |
|6   |
|0   |
|4   |
|0   |
|1   |
|6   |
|4   |
|1   |
|4   |
|0   |
|1   |
|0   |
|1   |
|0   |
|0   |
|0   |
|0   |
|0   |
|1   |
|1   |
|1   |
|2   |
|1   |
|1   |
|0   |
|1   |
|1   |
|1   |
|1   |
|1   |
|0   |
|0   |
|0   |
|1   |
|1   |
|2   |
|1   |
|1   |
|1   |
|0   |
|6   |
|1   |
|1   |
|4   |
|0   |
|6   |
|0   |
|0   |
|0   |
|6   |
|2   |
|1   |
|0   |
|1   |
|0   |
|4   |
|0   |
|2   |
|0   |
|4   |
|1   |
|1   |
|0   |
|1   |
|1   |
|1   |
|2   |
|4   |
|1   |
|6   |
|4   |
|0   |
|1   |
|6   |
|0   |
|1   |
|2   |
|0   |
|1   |
|0   |
|1   |
|2   |
|1   |
|6   |
|2   |
|2   |
|0   |
|1   |
|0   |
|1   |
|4   |
|1   |
|6   |
|0   |
|6   |
|4   |
|1   |
|4   |
|6   |
|6   |
|2   |
|0   |
|2   |
|6   |
|1   |
|0   |
|0   |
|1   |
|1   |
|0   |
|0   |
|0   |
|0   |
|0   |
|1   |
|0   |
|0   |
|0   |
|0   |
|1   |
|0   |
|0   |
|0   |
|1   |
|2   |
|1   |
|0   |
|0   |
|0   |
|0   |
|1   |
|1   |
|0   |
|6   |
|0   |
|0   |
|0   |
|0   |
|0   |
|1   |
|1   |
|0   |
|0   |
|0   |
|1   |
|0   |
|0   |
|0   |
|0   |
|4   |
|1   |
|0   |
|1   |
|2   |
|0   |
|0   |
|0   |
|0   |
|0   |
|0   |
|0   |
|1   |
|2   |
|0   |
|1   |
|0   |
|0   |
|0   |
|0   |
|4   |
|1   |
|0   |
|0   |
|0   |
|1   |
|4   |
|0   |
|1   |
|0   |
|0   |
|1   |
|0   |
|1   |
|6   |
|0   |
|1   |
|1   |
|0   |
|0   |
|1   |
|0   |
|1   |
|0   |
|0   |
|0   |
|6   |
|0   |
|0   |
|0   |
|1   |
|0   |
|1   |
|0   |
|0   |
|0   |
+----+


