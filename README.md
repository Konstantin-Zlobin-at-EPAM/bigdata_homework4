# BigData Training, Homework #3

## To count amount of all the tags in the dataset, please, run: UserTagCountTool
### How to run, examples:
On local env:
> hadoop jar target/homework3-1.0.jar com.epam.bigdata.usertag.UserTagCountTool /apps/homework3/dataset/stream.20130607-al.txt /apps/homework3/user.profile.tags.us.txt out

On a cluster:
> yarn jar homework3-1.0.jar com.epam.bigdata.usertag.UserTagCountTool -fs hdfs://big-azure:8020 -jt big-azure:8021 hdfs:///apps/homework3/dataset hdfs:///apps/homework3/user.profile.tags.us.txt userTagsShort

## To count amount of visits (count(*)) by IP and spends (sum(Bidding price)) by IP, please, run: VSByIPCountTool
### How to run, examples:
On local env:
> hadoop jar target/homework3-1.0.jar com.epam.bigdata.visitsspends.VSByIPCountTool /apps/homework3/dataset/stream.20130607-al.txt out

On a cluster:
> yarn jar homework3-1.0.jar com.epam.bigdata.visitsspends.VSByIPCountTool -fs hdfs://big-azure:8020 -jt big-azure:8021 hdfs:///apps/homework3/dataset VSByIPShort

## please note, output of VSByIPCountTool can be compressed and saved in a binary sequence format
## in order to enable this feature, please, add ".bin" to the name of the output folder, "out.bin" for instance.

