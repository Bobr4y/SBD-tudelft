# Processing the GDELT dataset on AWS
In this blog post we will evaluate the GDELT dataset on AWS elastic mapReduce (EMR). The goal is to process the dataset consisting of 157378 segments on 20 `c4.8xlarge` core nodes in under 30 minutes.

## Tweaking the code to run on AWS
- Move .jar and segments to s3
- Remove .master('local') from code
- Change paths to s3
- Option to read segments directly form GDELT s3

## Test run 100 segments
After adapting our code, a test run was done on 100 segments using 3 `m4.large` machines (master and 2 nodes). The step in EMR processing the 100 segments took 2 minutes to run. The resulting .json file can be seen below.

```
{"date":"2015-02-20","collect_list(named_struct(NamePlaceholder(), topic, NamePlaceholder(), count))":[{"topic":"United States","count":1975},{"topic":"Islamic State","count":1958},{"topic":"New York","count":1395},{"topic":"Cyclone Marcia","count":935},{"topic":"Los Angeles","count":823},{"topic":"United Kingdom","count":775},{"topic":"White House","count":713},{"topic":"Associated Press","count":702},{"topic":"Practice Wrestling Room","count":630},{"topic":"Softball Spring Practice Varsity","count":558}]}

{"date":"2015-02-19","collect_list(named_struct(NamePlaceholder(), topic, NamePlaceholder(), count))":[{"topic":"United States","count":20335},{"topic":"Islamic State","count":15411},{"topic":"New York","count":13131},{"topic":"United Kingdom","count":12926},{"topic":"White House","count":8622},{"topic":"Los Angeles","count":7125},{"topic":"Practice Wrestling Room","count":5810},{"topic":"Associated Press","count":5744},{"topic":"New Zealand","count":5509},{"topic":"Softball Spring Practice Varsity","count":5173}]}

{"date":"2015-02-18","collect_list(named_struct(NamePlaceholder(), topic, NamePlaceholder(), count))":[{"topic":"Islamic State","count":1787},{"topic":"United States","count":1210},{"topic":"New York","count":727},{"topic":"White House","count":489},{"topic":"Los Angeles","count":424},{"topic":"Associated Press","count":385},{"topic":"New Zealand","count":353},{"topic":"United Kingdom","count":325},{"topic":"Jeb Bush","count":298},{"topic":"Practice Wrestling Room","count":280}]}
```

From this .json file it can be seen that all the segments of 19-02-2015 have been processed, while this is probably not the case for 18-02 and 20-02 since these counts are lower.

This was a preliminary test and it is now time to see how we perform on more segments.

## Processing 1000 segments