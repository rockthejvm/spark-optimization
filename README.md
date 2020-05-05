# The official repository for the Rock the JVM Spark Optimization with Scala course

Powered by [Rock the JVM!](rockthejvm.com)

This repository contains the code we wrote during [Rock the JVM's Spark Optimization with Scala](https://rockthejvm.com/course/spark-optimization) course. Unless explicitly mentioned, the code in this repository is exactly what was caught on camera.

### Install and setup

- install [IntelliJ IDEA](https://jetbrains.com/idea)
- install [Docker Desktop](https://docker.com)
- either clone the repo or download as zip
- open with IntelliJ as an SBT project

As you open the project, the IDE will take care to download and apply the appropriate library dependencies.

To set up the dockerized Spark cluster we will be using in the course, do the following:

- open a terminal and navigate to `spark-cluster`
- run `build-images.sh` (if you don't have a bash terminal, just open the file and run each line one by one)
- run `docker-compose up`

To interact with the Spark cluster, the folders `data` and `apps` inside the `spark-cluster` folder are mounted onto the Docker containers under `/opt/spark-data` and `/opt/spark-apps` respectively.

To run a Spark shell, first run `docker-compose up` inside the `spark-cluster` directory, then in another terminal, do

```
docker exec -it spark-cluster_spark-master_1 bash
```

and then

```
/spark/bin/spark-shell
```

### How to use intermediate states of this repository

Start by cloning this repository and checkout the `start` tag:

```
git checkout start
```

### How to run an intermediate state

The repository was built while recording the lectures. Prior to each lecture, I tagged each commit so you can easily go back to an earlier state of the repo!

The tags are as follows:

* `start`
* `1.1-scala-recap`
* `1.2-spark-recap`
* `2.2-spark-job-anatomy`
* `2.3-query-plans`
* `2.3-query-plans-exercises`
* `2.4-spark-ui`
* `2.5-spark-apis`
* `2.6-deploy-config`
* `3.1-join-mechanics`
* `3.2-broadcast-joins`
* `3.3-column-pruning`
* `3.4-prepartitioning`
* `3.5-bucketing`
* `3.6-skewed-joins`
* `4.1-rdd-joins`
* `4.2-cogroup`
* `4.3-rdd-broadcast`
* `4.4-rdd-skews`
* `5.1-rdd-transformations`
* `5.2-by-key-ops`
* `5.3-reusing-objects`
* `5.5-i2i-transformations`
* `5.6-i2i-transformations-exercises`

When you watch a lecture, you can `git checkout` the appropriate tag and the repo will go back to the exact code I had when I started the lecture.

### For questions or suggestions

If you have changes to suggest to this repo, either
- submit a GitHub issue
- tell me in the course Q/A forum
- submit a pull request!
