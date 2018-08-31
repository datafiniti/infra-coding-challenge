# Coding Challenge

## Introduction

At Datafiniti, we have several millions of product records in our database that we've collected from several retailers on the internet. We're tasked with keeping those records as up-to-date as we can. This means our data pipeline needs to be blazing fast with regards to getting data from the internet into our database. This coding challenge exposes you to a miniature prototype of our pipeline and tasks you with speeding up the rate at which data moves through it. 

## Objective
To reiterate, our goal is to get data, in the form of json objects, from point A to point B as fast as we possibly can. These json objects begin by being enqueued in a shared in-memory cache (redis in this case). We provide you with code that simply dequeues records one at a time and imports them into elasticsearch. Your objective is to do whatever it takes to increase the rate at which records are inserted into elasticsearch. You can complete this challenge by improving either the java or nodejs code we provide. Feel free to change any part of this codebase or introduce/replace technologies in order to increase import rates. The only rule of this challenge is that records must be enqueued somewhere and then imported into elasticsearch. This may seem initially daunting but don't worry, we provide plenty of tools which we explain in the section below.

## Setup Instructions

### Prerequistes
- Docker
  - If you don't already have docker you can download and install it from the following links:
    - [mac](https://www.docker.com/docker-mac)
    - [windows](https://www.docker.com/docker-windows)
    - [linux](https://runnable.com/docker/install-docker-on-linux)
  - For those of you on Windows or a Mac: increase the memory allocated to docker to at least 4GB. This setting can be found within docker's preferences. Feel free to reach out if you don't have that much memory available on your machine. Linux users don't need to worry about this because there's no virtual machine running between your host OS and your docker containers.

- Fork this repository, clone it down and cd into it.
- The repository contains a docker composition that sets up the following docker containers:
  - Redis
  - Elasticsearch
  - Kibana. This is a fancy web UI that you can use to monitor import metrics in order to benchmark your solution
  - A dev container with either java or nodejs installed depending on which you decide to use
- Follow the instructions below for your language of choice


### NodeJS
![Instructions Screencast](https://github.com/datafiniti/infra-coding-challenge/blob/master/screencasts/nodejs-screencast.gif)

1. From within the `coding-challenge` directory, run `./bin/setup-nodejs.sh`
2. Once the output is done printing to the terminal click on this [link](http://localhost:5601).
3. Click the Monitoring button on the far left
4. Click `Enable Monitoring`
5. Open another terminal and type in `docker exec -it node-dev bash`
   - This is equivalent to `ssh`ing into a virtual machine that has nodejs installed
6. To run the code we've provided run the following commands

   ```bash
   cd code
   npm install
   node baseline.js
   ```
   - This code will seed 10,000 product records into redis and then begin to import one record at a time into elasticsearch
7. Head over to the browser window that you have kibana open in
8. Click on indices and then records
9. Watch the Indexing Rate graph to see how fast the provided solution is.
   - You'll be using this graph to benchmark your solution
   - Your solution will be assessed by first running `baseline.js` and then your solution. If the graph indicates that your solution is indexing records faster than the baseline then Congratulations! you did it.
10. Provide your solution in a file named `solution.js`.

### Java
![Instructions Screencast](https://github.com/datafiniti/infra-coding-challenge/blob/master/screencasts/java-screencast.gif)
1. From within the `coding-challenge` directory, run `./bin/setup-java.sh`
2. Once the output is done printing to the terminal click on this [link](http://localhost:5601).
3. Click the Monitoring button on the far left
4. Click `Enable Monitoring`
5. Open another terminal and type in `docker exec -it java-dev bash`
   - This is equivalent to `ssh`ing into a virtual machine that has java installed
6. To run the code we've provided run the following commands

   ```bash
   cd code

   # This command will compile the baseline solution we provide to you and run it.
   ./bin/run.sh
   ```
   - This code will seed 10,000 product records into redis and then begin to import one record at a time into elasticsearch
7. Head over to the browser window that you have kibana open in
8. Click on indices and then records
9. Watch the Indexing Rate graph to see how fast the provided solution is.
   - You'll be using this graph to benchmark your solution
   - Your solution will be assessed by first running `Baseline` and then your solution. If the graph indicates that your solution is indexing records faster than the baseline then Congratulations! you did it.
10. Provide your solution in a class named `Solution`. Make sure that we can run your solution from the `Main` class.
    - You can run your code using `./bin/run.sh` so long as you run your solution from within `Main`


## Cleanup and Submission
- Once you're done, from within the `coding-challenge` directory, run `./bin/teardown.sh` in order to delete all containers and volumes
- Send over an email with a link to your forked repo and we'll take a look ASAP!

## Hints
- Speeding up the import rate does not require some fancy algorithm or data strucuture. 