###### IMPORT LIBRARIES AND SET NECESSARY CONFIGURATION FOR SparkContext and SparkSession ###########
from pyspark.sql import SparkSession
spark = SparkSession\
       .builder\
       .appName("UI Data Prep.py")\
       .enableHiveSupport()\
       .getOrCreate()
sc = spark.sparkContext
import sys
import argparse
import warnings
import pandas as pd
from pyspark.sql.types import IntegerType, DoubleType, DateType, StringType
from pyspark.sql import SparkSession, Window
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql.window import Window
from pyspark.sql import functions as psf
from pyspark.sql.types import *
from pyspark.mllib.stat import Statistics
from pyspark.sql.types import StructType
from pyspark.ml.classification import *
from pyspark.sql.functions import udf,col,desc
import dateutil.relativedelta

##### JDBC COnfiguration to directly write Spark Dataframe to SQL Server ####
url = "jdbc:jtds:sqlserver://10.1.3.5:1433/CIPVizDb"
properties = {
    "driver": "net.sourceforge.jtds.jdbc.Driver",
    "user": "pwcsqladmin",
    "password": "Sgls02112017$"
}

##### Loading relevant data required for preparing tables
parser = argparse.ArgumentParser()
parser.add_argument('-f', nargs='?')
parser.add_argument('--ClientRevenueLocation', nargs = '?', default = "hdfs://10.182.208.244:8020/tmp/USFS/FS_Banking/cli/ClientRevenue.parquet", help = 'Path to Client Revenue Table')
parser.add_argument('--ProductDictionaryLocation', nargs = '?', default = "hdfs://10.182.208.244:8020/tmp/USFS/FS_Banking/dict/Product.parquet", help = 'Path to Product Dictionary Table')
parser.add_argument('--SegmentationLocation', nargs = '?', default = "hdfs://10.182.208.244:8020/tmp/USFS/FS_Banking/segmentation/SegmentationScore.parquet", help = 'Path to SegmentationScore Table')
parser.add_argument('--ClientBusinessSegmentLocation', nargs = '?', default = "hdfs://10.182.208.244:8020/tmp/USFS/FS_Banking/cli/ClientBusinessSegment.parquet", help = 'Path to ClientBusinessSegment Table')
parser.add_argument('--ClientLocation', nargs = '?', default = "hdfs://10.182.208.244:8020/tmp/USFS/FS_Banking/cli/Client.parquet", help = 'Path to Client Table')
parser.add_argument('--CLVDataLocation', nargs = '?', default = "hdfs://10.182.208.244:8020/tmp/USFS/FS_Banking/var/CLVOutputs/ClientCLVProjection", help = 'Path to CLV Data Table')
parser.add_argument('--TriggerLocation', nargs = '?', default = "hdfs://10.182.208.244:8020/tmp/USFS/FS_Banking/var/TriggerTable.parquet", help = 'Path to Trigger Table')
parser.add_argument('--DictSegmentationLocation', nargs = '?', default = "hdfs://10.182.208.244:8020/tmp/USFS/FS_Banking/segmentation/dictSegmentationScore.parquet", help = 'Path to Segmentation Dictionary Table')
parser.add_argument('--DictScoringLocation', nargs = '?', default = "hdfs://10.182.208.244:8020/tmp/USFS/FS_Banking/model/dictScoring.parquet", help = 'Path to Scoring Dictionary Table')
parser.add_argument('--TransitionMatrixLocation', nargs = '?', default = "hdfs://10.182.208.244:8020/tmp/USFS/FS_Banking/var/CLVOutputs/CampaignTransitionMatrix.parquet", help = 'Path to Transition Matrix Table')
parser.add_argument('--TriggerDetailsLocation', nargs = '?', default = "hdfs://10.182.208.244:8020/tmp/USFS/FS_Banking/var/Trigger_Details.parquet", help = 'Path to Trigger Details Table')
parser.add_argument('--SampleID', nargs = '?', default = 1, help = 'Data on which the model was run')
parser.add_argument('--SegID', nargs = '?', default = 1, help = 'Segmentation ID to used for processing data')
parser.add_argument('--ModelID', nargs = '?', default = 2, help = 'Model ID to be used from Scored Data')

inP = parser.parse_args()

ClientRevenueLocation = inP.ClientRevenueLocation
ProductDictionaryLocation = inP.ProductDictionaryLocation
SegmentationLocation = inP.SegmentationLocation
ClientBusinessSegmentLocation = inP.ClientBusinessSegmentLocation
ClientLocation = inP.ClientLocation
CLVDataLocation = inP.CLVDataLocation
TriggerLocation = inP.TriggerLocation
DictSegmentationLocation = inP.DictSegmentationLocation
DictScoringLocation = inP.DictScoringLocation
TransitionMatrixLocation = inP.TransitionMatrixLocation
TriggerDetailsLocation = inP.TriggerDetailsLocation
SampleID = inP.SampleID
SegID = inP.SegID
ModelID = inP.ModelID

##### Reading the data from file location, Filtering and registering as Temp table
Data = spark.read.parquet(DictScoringLocation)
ScoredData = Data.filter(Data.SampleID == SampleID)
ScoredData.registerTempTable("ScoredData")

SegmentationScore = spark.read.parquet(SegmentationLocation)
SegmentationScore.registerTempTable('SegmentationScore')
SegmentationScore = SegmentationScore.filter(SegmentationScore.SegScoreID==SegID)

Client = spark.read.parquet(ClientLocation)
Client.registerTempTable('Client')

ClientBusinessSegment = spark.read.parquet(ClientBusinessSegmentLocation)
ClientBusinessSegment.registerTempTable('ClientBusinessSegment')

ClientRevenue = spark.read.parquet(ClientRevenueLocation)
ClientRevenue.registerTempTable('ClientRevenue')

ProductIDDict = spark.read.parquet(ProductDictionaryLocation)
ProductIDDict.registerTempTable('ProductIDDict')

CLVData = spark.read.parquet(CLVDataLocation)
CLVData.registerTempTable('CLVData')

TriggerData = spark.read.parquet(TriggerLocation)
TriggerData.registerTempTable('TriggerData')

DictSegmentation = spark.read.parquet(DictSegmentationLocation)
DictSegmentation.registerTempTable('DictSegmentation')

TransitionMatrix = spark.read.parquet(TransitionMatrixLocation)
TransitionMatrix.registerTempTable('TransitionMatrix')

Trigger_Details = spark.read.parquet(TriggerDetailsLocation)
Trigger_Details.registerTempTable('Trigger_Details')

#### Data Preparation for Landing Page ####
date_max = ClientRevenue.select("ReportingDate").rdd.max()[0]
date_max = date_max.strftime('%Y-%m-%d')
ClientTotalRevenue = spark.sql("SELECT ClientID,ReportingDate,sum(Revenue) AS sum_Revenue from ClientRevenue group by 1,2")
ClientTotalRevenue = ClientTotalRevenue.orderBy("ClientID","ReportingDate")

#Create 3 month flags
import sys
from pyspark.sql.window import Window
from pyspark.sql.functions import exp, log, sum, first, col, coalesce

# Base window
w = Window.partitionBy("ClientID").orderBy("ReportingDate")
# ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
wr = w.rowsBetween(-2, 0)
# Take a sum of sum_Revenue over the window
sum_3 = sum((col("sum_Revenue"))).over(wr)
# # Prepare final column showing total revenue in the last 3 months for each ClientID and ReportingDate
sum_last3months = coalesce(sum_3).alias("sum_last3months")
# ClientTotalRevenue.select("*", sum_last3months).show(4)     
# ROWS BETWEEN 5 PRECEDING AND 3 PRECEDING ROWS
wr = w.rowsBetween(-5, -3)
# Take a sum of sum_Revenue over the window
sum_3_1 = sum((col("sum_Revenue"))).over(wr)
# Prepare final column showing total revenue for each clientID and ReportingDate in the previous 3 months (the 3 months before the last 3 months)
sum_previous3months = coalesce(sum_3_1).alias("sum_previous3months")
# ClientTotalRevenue.select("*", sum_previous3months).show(4)
ClientTotalRevenue1=ClientTotalRevenue.select("*",sum_last3months,sum_previous3months)
ClientTotalRevenue1.registerTempTable("ClientTotalRevenue1")
# ClientTotalRevenue1.show(5)
#Calculate the difference in total revenue in the last 3 months and previous 3 months to determine grow flags
col_name1="sum_last3months"
col_name2="sum_previous3months"
ClientTotalRevenue2 = ClientTotalRevenue1.withColumn('Difference_in_sums',(ClientTotalRevenue1[col_name1]-ClientTotalRevenue1[col_name2]))
ClientTotalRevenue2.registerTempTable("ClientTotalRevenue2")
# ClientTotalRevenue2.show(4)
#Creating columns showing whether the client is a 'Grow Client' at the given ReportingDate
ClientTotalRevenue_3_df = ClientTotalRevenue2.withColumn("is_grow_3months", ClientTotalRevenue2.Difference_in_sums > 0)
ClientTotalRevenue_3_df1 = ClientTotalRevenue_3_df.withColumn("Flag_3months", ClientTotalRevenue_3_df.is_grow_3months.cast("integer"))
# ClientTotalRevenue_3_df1.show(100)
# ClientTotalRevenue_3_df1.dtypes
ClientTotalRevenue_3_df2 = ClientTotalRevenue_3_df1.withColumn("is_grow_3months", ClientTotalRevenue_3_df1.is_grow_3months.cast("integer"))
# ClientTotalRevenue_3_df2.dtypes
ClientTotalRevenue_3_df2.registerTempTable("ClientTotalRevenue_3_df2")
# ClientTotalRevenue_3_df2.show(20)

#Create 6 month flags
# Base window
w = Window.partitionBy("ClientID").orderBy("ReportingDate")
# ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
wr = w.rowsBetween(-5, 0)
# Take a sum of sum_Revenue over the window
sum_6 = sum((col("sum_Revenue"))).over(wr)
# Prepare final column 
sum_last6months = coalesce(sum_6).alias("sum_last6months")
# ClientTotalRevenue.select("*", sum_last6months).show(5)
# ROWS BETWEEN 11 PRECEDING AND 6 PRECEDING ROWS
wr = w.rowsBetween(-11, -6)
# Take a sum of sum_Revenue over the window
sum_6_1 = sum((col("sum_Revenue"))).over(wr)
# Prepare final column 
sum_previous6months = coalesce(sum_6_1).alias("sum_previous6months")
# ClientTotalRevenue.select("*",sum_previous6months ).show(5)
ClientTotalRevenue3=ClientTotalRevenue.select("*", sum_last6months, sum_previous6months)
ClientTotalRevenue3.registerTempTable("ClientTotalRevenue3")
# ClientTotalRevenue3.show(2)
col_name1="sum_last6months"
col_name2="sum_previous6months"
ClientTotalRevenue4 = ClientTotalRevenue3.withColumn('Difference_in_sums',(ClientTotalRevenue3[col_name1]-ClientTotalRevenue3[col_name2]))
ClientTotalRevenue4.registerTempTable("ClientTotalRevenue4")
# ClientTotalRevenue4.show(4)
ClientTotalRevenue_6_df = ClientTotalRevenue4.withColumn("is_grow_6months", ClientTotalRevenue4.Difference_in_sums > 0)
ClientTotalRevenue_6_df1 = ClientTotalRevenue_6_df.withColumn("Flag_6months", ClientTotalRevenue_6_df.is_grow_6months.cast("integer"))
# ClientTotalRevenue_6_df1.show(100)
ClientTotalRevenue_6_df2 = ClientTotalRevenue_6_df1.withColumn("is_grow_6months", ClientTotalRevenue_6_df1.is_grow_6months.cast("integer"))
ClientTotalRevenue_6_df2.registerTempTable("ClientTotalRevenue_6_df2")
# ClientTotalRevenue_6_df2.show(40)

#Create 9 month flags
# Base window
w = Window.partitionBy("ClientID").orderBy("ReportingDate")
# ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
wr = w.rowsBetween(-8, 0)
# Take a sum of sum_Revenue over the window
sum_9 = sum((col("sum_Revenue"))).over(wr)
# Prepare final column 
sum_last9months = coalesce(sum_9).alias("sum_last9months")
# ClientTotalRevenue.select("*", sum_last9months).show(5)
# ROWS BETWEEN 17 PRECEDING AND 9 PRECEDING ROWS
wr = w.rowsBetween(-17, -9)
# Take a sum of sum_Revenue over the window
sum_9_1 = sum((col("sum_Revenue"))).over(wr)
# Prepare final column 
sum_previous9months = coalesce(sum_9_1).alias("sum_previous9months")
# ClientTotalRevenue.select("*", sum_previous9months).show(5)
ClientTotalRevenue5=ClientTotalRevenue.select("*", sum_last9months, sum_previous9months)
ClientTotalRevenue5.registerTempTable("ClientTotalRevenue5")
# ClientTotalRevenue5.show(2)
col_name1="sum_last9months"
col_name2="sum_previous9months"
ClientTotalRevenue6 = ClientTotalRevenue5.withColumn('Difference_in_sums',(ClientTotalRevenue5[col_name1]-ClientTotalRevenue5[col_name2]))
ClientTotalRevenue6.registerTempTable("ClientTotalRevenue6")
# ClientTotalRevenue6.show(4)
ClientTotalRevenue_9_df = ClientTotalRevenue6.withColumn("is_grow_9months", ClientTotalRevenue6.Difference_in_sums > 0)
ClientTotalRevenue_9_df1 = ClientTotalRevenue_9_df.withColumn("Flag_9months", ClientTotalRevenue_9_df.is_grow_9months.cast("integer"))
# ClientTotalRevenue_9_df1.show(100)
ClientTotalRevenue_9_df2 = ClientTotalRevenue_9_df1.withColumn("is_grow_9months", ClientTotalRevenue_9_df1.is_grow_9months.cast("integer"))
ClientTotalRevenue_9_df2.registerTempTable("ClientTotalRevenue_9_df2")
# ClientTotalRevenue_9_df2.show(50)

#Create 12 month flags
# Base window
w = Window.partitionBy("ClientID").orderBy("ReportingDate")
# ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
wr = w.rowsBetween(-11, 0)
# Take a sum of sum_Revenue over the window
sum_12 = sum((col("sum_Revenue"))).over(wr)
# Prepare final column 
sum_last12months = coalesce(sum_12).alias("sum_last12months")
# ClientTotalRevenue.select("*", sum_last12months).show(5)
# ROWS BETWEEN 23 PRECEDING AND 12 PRECEDING ROWS
wr = w.rowsBetween(-23, -12)
# Take a sum of sum_Revenue over the window
sum_12_1 = sum((col("sum_Revenue"))).over(wr)
# Prepare final column 
sum_previous12months = coalesce(sum_12_1).alias("sum_previous12months")
# ClientTotalRevenue.select("*", sum_previous12months).show(5)
ClientTotalRevenue7=ClientTotalRevenue.select("*", sum_last12months, sum_previous12months)
ClientTotalRevenue7.registerTempTable("ClientTotalRevenue7")
# ClientTotalRevenue7.show(4)
col_name1="sum_last12months"
col_name2="sum_previous12months"
ClientTotalRevenue8 = ClientTotalRevenue7.withColumn('Difference_in_sums',(ClientTotalRevenue7[col_name1]-ClientTotalRevenue7[col_name2]))
ClientTotalRevenue8.registerTempTable("ClientTotalRevenue8")
# ClientTotalRevenue8.show(4)
ClientTotalRevenue_12_df = ClientTotalRevenue8.withColumn("is_grow_12months", ClientTotalRevenue8.Difference_in_sums > 0)
ClientTotalRevenue_12_df1 = ClientTotalRevenue_12_df.withColumn("Flag_12months", ClientTotalRevenue_12_df.is_grow_12months.cast("integer"))
# ClientTotalRevenue_12_df1.show(100)
ClientTotalRevenue_12_df2 = ClientTotalRevenue_12_df1.withColumn("is_grow_12months", ClientTotalRevenue_12_df1.is_grow_12months.cast("integer"))
ClientTotalRevenue_12_df2.registerTempTable("ClientTotalRevenue_12_df2")
# ClientTotalRevenue_12_df2.show(60)

#Join the 4 dataframes to obtain a dataframe with ClientID, ReportingDate, sum_Revenue amd 3,6,9,12 month flags only
Data = spark.sql("SELECT ctr3.ClientID, ctr3.ReportingDate, ctr3.sum_Revenue, ctr3.Flag_3months, ctr6.Flag_6months \
                  FROM ClientTotalRevenue_3_df2 ctr3 LEFT JOIN ClientTotalRevenue_6_df2 ctr6 ON \
                  ctr3.ClientID = ctr6.ClientID AND ctr3.ReportingDate = ctr6.ReportingDate")
Data.registerTempTable("Data")
Data1 = spark.sql("SELECT d.ClientID, d.ReportingDate,d.sum_Revenue, d.Flag_3months, d.Flag_6months, ctr9.Flag_9months \
                  FROM Data d LEFT JOIN ClientTotalRevenue_9_df2 ctr9 ON \
                  d.ClientID = ctr9.ClientID AND d.ReportingDate = ctr9.ReportingDate")
Data1.registerTempTable("Data1")
Data2 = spark.sql("SELECT d1.ClientID, d1.ReportingDate, d1.sum_Revenue , d1.Flag_3months, d1.Flag_6months, d1.Flag_9months, ctr12.Flag_12months \
                  FROM Data1 d1 LEFT JOIN ClientTotalRevenue_12_df2 ctr12 ON \
                  d1.ClientID = ctr12.ClientID AND d1.ReportingDate = ctr12.ReportingDate")
Data2.registerTempTable("Data2")
#Filter for max ReportingDate
FinalData = spark.sql("SELECT * FROM Data2 WHERE ReportingDate == '" + date_max + "'")
FinalData.registerTempTable("FinalData")
# FinalData.show(40)
FinalData.write.jdbc(url=url, table="dbo.LandingPageData_Grow_Flags", mode="overwrite", properties=properties)

#### Saving Transition Matrix as a Table ####
TransitionM = spark.sql("SELECT CLV_Cohort_From, CLV_Cohort_To, avg(ProbabilityBefore) AS probability \
                         FROM TransitionMatrix \
                         GROUP BY CLV_Cohort_From, CLV_Cohort_To")
TransitionM.write.jdbc(url=url, table="dbo.TransitionMatrix", mode="overwrite", properties=properties)

#Identifying the current clients using the latest ReportingDate
#Subsetting the ScoredData to contain details of only CurrentClients
CurrentClientData = spark.sql("SELECT * FROM ScoredData WHERE ModelID = " + str(ModelID) + " AND ClientID IN (SELECT ClientID FROM ScoredData WHERE ReportingDate = (SELECT MAX(ReportingDate) FROM ScoredData))")
CurrentClientData.registerTempTable("CurrentClientData")

#CREATION OF TABLE 1
Table0 = spark.sql("SELECT ClientID, ReportingDate, ModelID, Scores AS ChurnScore FROM CurrentClientData")
Table0.persist()
# firstelement=udf(lambda v:float(v[1]),FloatType())
Table1= Table0.select('ClientID', 'ReportingDate', 'ModelID','ChurnScore')
Table1.write.jdbc(url=url, table="dbo.Churn_Data", mode="overwrite", properties=properties)
Table0.unpersist()

#CREATION OF TABLE 2
Table2 = spark.sql("SELECT ClientID, ReportingDate, Flag AS ChurnFlag FROM CurrentClientData")
Table2.write.jdbc(url=url, table="dbo.ChurnFlag", mode="overwrite", properties=properties)

Table1.registerTempTable('Table1')
Table2.registerTempTable('Table2')

Revenue = spark.sql("SELECT \
                     r.* \
                     ,p.ProductName \
                     ,p.ProductID \
                 FROM \
                     ClientRevenue r \
                 LEFT JOIN ProductIDDict p \
                     on p.ProductID = r.ProductID \
                 ")
Revenue.registerTempTable('Revenue')
Revenue_Data = spark.sql("SELECT ClientID, ReportingDate, SUM(Revenue) AS Revenue FROM Revenue GROUP BY ClientID, ReportingDate  ")
Revenue_Data.registerTempTable('Revenue_Data')
#Revenue_Data.write.jdbc(url=url, table="dbo.Rev_Data", mode="overwrite", properties=properties)

Product_Revenue = spark.sql("SELECT ClientID, ReportingDate, ProductID AS Product, Revenue, Revenue*0.3 AS Profit \
                             FROM Revenue \
                            ")
Product_Revenue.registerTempTable('Product_Revenue')
Product_Revenue.write.jdbc(url=url, table="dbo.Prod_Rev", mode="overwrite", properties=properties)

CLVData_Hist = CLVData.filter(CLVData.TypeOfObservation == 'Historical')
CLVData_Fut = CLVData.filter(CLVData.TypeOfObservation == 'Future')
CLVData_Hist.registerTempTable('CLVData_Hist')
CLVData_Fut.registerTempTable('CLVData_Fut')

CLV_Data_1 = spark.sql("SELECT clv.ClientID, clv.ReportingDate, clv.MicroSegmentID AS CLV_Cohort, clv.CurrentCLV AS CLV, \
                        cbs.BusinessSegmentID, ss.ClusterID AS BehavioralSegment  \
                        FROM CLVData clv \
                        LEFT JOIN ClientBusinessSegment cbs ON clv.ClientID = cbs.ClientID AND clv.ReportingDate = cbs.ReportingDate \
                        LEFT JOIN SegmentationScore ss ON clv.ClientID = ss.ClientID AND clv.ReportingDate = ss.ReportingDate\
                        WHERE clv.ReportingDate < (SELECT min(ReportingDate) FROM CLVData_Fut) \
                      ")
CLV_Data_1.registerTempTable('CLV_Data_1')
CLV_Data_1.persist()

cbs2 = spark.sql("SELECT ClientID, BusinessSegmentID FROM ClientBusinessSegment \
                  WHERE ReportingDate = (SELECT max(ReportingDate) FROM CLVData_Hist)")
cbs2 = cbs2.dropDuplicates()
cbs2.registerTempTable('cbs2')

ss = spark.sql("SELECT ClientID, ClusterID FROM SegmentationScore\
                WHERE ReportingDate = (SELECT max(ReportingDate) FROM CLVData_Hist)\
                ")
ss2 = ss.dropDuplicates()
ss2.registerTempTable('ss2')

CLV_Data_2 = spark.sql("SELECT clv.ClientID, clv.ReportingDate, clv.MicroSegmentID AS CLV_Cohort, clv.CurrentCLV AS CLV, \
                        cbs2.BusinessSegmentID, ss2.ClusterID AS BehavioralSegment  \
                        FROM CLVData clv \
                        LEFT JOIN cbs2 ON clv.ClientID = cbs2.ClientID \
                        LEFT JOIN ss2 ON clv.ClientID = ss2.ClientID \
                        WHERE clv.ReportingDate >= (SELECT min(ReportingDate) FROM CLVData_Fut) \
                      ")
CLV_Data_2.registerTempTable('CLV_Data_2')
CLV_Data_2.persist()

CLV_Data = spark.sql("SELECT * FROM CLV_Data_1 \
                      UNION \
                      SELECT * FROM CLV_Data_2"
                    )
CLV_Data.write.jdbc(url=url, table="dbo.CLV_Data", mode="overwrite", properties=properties)
CLV_Data.registerTempTable('CLV_Data')
CLV_Data_1.unpersist()
CLV_Data_2.unpersist()

#### Client_Time0 Table ####
CLV_DataT0 = spark.sql("SELECT * FROM CLVData WHERE ReportingDate = (SELECT max(ReportingDate) FROM CLVData_Hist) ")
CLV_DataT0.registerTempTable('CLV_DataT0')
C = spark.sql("SELECT ClientID, max(PermanentAddressStateID) AS PermanentAddressStateID, \
               max(PermanentAddressZIPID) AS PermanentAddressZIPID FROM Client\
               GROUP BY ClientID")
C.registerTempTable('C')
Client_Time0 = spark.sql("SELECT clv.ClientID, clv.ReportingDate, clv.MicroSegmentID AS CLV_Cohort, clv.CurrentCLV AS CLV, \
                             ss.ClusterID AS BehavioralSegment, cbs.BusinessSegmentID, \
                             FLOOR(RAND()*(51-1)+1) AS PermanentAddressStateID, \
                             FLOOR(RAND()*(43000-1)+1) AS PermanentAddressZIPID \
                             FROM CLV_DataT0 clv \
                             LEFT JOIN ClientBusinessSegment cbs ON clv.ClientID = cbs.ClientID AND clv.ReportingDate = cbs.ReportingDate \
                             LEFT JOIN SegmentationScore ss ON clv.ClientID = ss.ClientID AND clv.ReportingDate = ss.ReportingDate\
                             LEFT JOIN C cl ON cl.ClientID = clv.ClientID \
                         ")
Client_Time0.registerTempTable('Client_Time0')
ClientRevenue_T0 = spark.sql("SELECT c.*, pr.Product FROM Client_Time0 c LEFT JOIN Product_Revenue pr ON \
                              c.ReportingDate = pr.ReportingDate AND c.ClientID = pr.ClientID") 
def assign_product(prod):
    return udf(lambda val: 1 if val==prod else 0)
disProducts = Product_Revenue.select("Product").distinct().rdd.flatMap(lambda x: x).collect()
for prod in disProducts:
    ClientRevenue_T0 = ClientRevenue_T0.withColumn("Product_"+str(prod), assign_product(prod)(col("Product")))
Client_Time0 = ClientRevenue_T0.drop("Product")
Client_Time0.write.jdbc(url=url, table="dbo.Client_Time0", mode="overwrite", properties=properties)

# Trigger_Details.write.jdbc(url=url, table="dbo.Trigger_Details", mode="overwrite", properties=properties)
# TriggerData.write.jdbc(url=url, table="dbo.TriggerData", mode="overwrite", properties=properties)

date_max = Revenue_Data.select("ReportingDate").rdd.max()[0]
date_min = date_max - dateutil.relativedelta.relativedelta(months=12)
date_min = date_min.strftime('%Y-%m-%d')
date_max = date_max.strftime('%Y-%m-%d')
AvgRevenue = spark.sql("SELECT avg(r.Revenue) AS Average_Revenue, c.CLV_Cohort \
                FROM CLV_Data c \
                LEFT JOIN Revenue_Data r ON r.ClientID = c.ClientID AND r.ReportingDate =  c.ReportingDate\
                WHERE c.ReportingDate BETWEEN '" + date_min + "' AND '" + date_max + "'\
                GROUP BY c.CLV_Cohort \
               ")
AvgRevenue.registerTempTable('AvgRevenue')
AR.write.jdbc(url=url, table="dbo.Avg_Rev_Cohort", mode="overwrite", properties=properties)

AvgCLVData = spark.sql("SELECT CLV_Cohort, avg(CLV) AS AvgCLV FROM CLV_Data \
                        WHERE ReportingDate = '" + date_max + "'\
                        GROUP BY CLV_Cohort")
AvgCLVData.write.jdbc(url=url, table="dbo.AvgCLVData", mode="overwrite", properties=properties)
