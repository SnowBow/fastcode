### A hadoop version to NgramCount, HashtagSim and KMeans
## Chen Wang, chenw@cmu.edu

1. Hadoop Version of NgramCount
  * Description: Give the count of all N-grams (http://en.wikipedia.org/wiki/N-gram) in a given documents. You could specify N.
  * Usage:
    - `hadoop jar 18645-kmeans-0.1-latest.jar –program ngramcount -input testdata -output testoutput -n N`

2. Hadoop version of HashtagSim
  * Description: Compute the similarities between all pairs of hashtags that share at least 1 common word.
  * Usage:
    - `hadoop jar 18645-kmeans--0.1-latest.jar –program hashtagsim -input testdata -output testoutput -tmpdir tmp`
    
3. Hadoop version of Kmeans
  * Description: Use hadoop to parallelize kmeans clustering algorithm.
  * Details: https://github.com/ephemeral2eternity/fastcode/blob/master/ProjDocs/18645-Final-Report.pdf
  * Usage:
    - `hadoop jar 18645-kmeans--0.1-latest.jar –program kmeans -input testdata -output testoutput -tmpdir tmp -cluster_num K`
