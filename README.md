# The official repository for the Rock the JVM Spark Optimization with Scala course

Powered by [Rock the JVM!](rockthejvm.com)

This repository contains the code we wrote during [Rock the JVM's Spark Optimization with Scala](https://rockthejvm.com/course/spark-optimization) course. Unless explicitly mentioned, the code in this repository is exactly what was caught on camera.

### Install and setup

- install [IntelliJ IDEA](https://jetbrains.com/idea)
- install [Docker Desktop](https://docker.com)
- either clone the repo or download as zip
- open with IntelliJ as an SBT project
- Windows users, you need to set up some Hadoop-related configs - use [this guide](/HadoopWindowsUserSetup.md) 

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

### For questions or suggestions

If you have changes to suggest to this repo, either
- submit a GitHub issue
- tell me in the course Q/A forum
- submit a pull request!
