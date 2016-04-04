# BigData Training, Homework #4

## To sort all the lines in the dataset first by iPinyou ID and then by Timestam 
## and to find iPinyou ID with the biggest amount of site-impression, please, run: BiggestImpressionTool
### How to run, examples:
On local env:
> hadoop jar target/homework4-1.0.jar com.epam.bigdata.impressions.BiggestImpressionTool /apps/homework3/dataset/stream.20130607-al.txt out

On a cluster:
> yarn jar homework4-1.0.jar com.epam.bigdata.impressions.BiggestImpressionTool -fs hdfs://big-azure:8020 -jt big-azure:8021 hdfs:///apps/homework3/dataset sortedByIdTimestampFull

## the biggest impression for given iPinyou ID can be found in the driver's output:
#### 16/04/04 19:58:07 INFO impressions.BiggestImpressionTool: Reducer counters:
#### 16/04/04 19:58:07 INFO impressions.BiggestImpressionTool: 	BIGGEST_IMPRESSIONS
#### 16/04/04 19:58:07 INFO impressions.BiggestImpressionTool: 	iPinId: Vhd6P7xtO9KVXfL = 10 (imprs.)


