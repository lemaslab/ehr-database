## Instructions on building a RED-I image 

We will start with a [ubuntu docker images](https://hub.docker.com/_/ubuntu) and install [RED-I](https://redi.readthedocs.io/en/latest/).

In the examples below, `$` indicates the command line prompt within the container.

### 1) open docker terminal

### 2) pull container
```
docker pull python:2.7.9
```

### 3) boot into container as bash
```
docker run -it python:2.7.9 bash
```

### 4) Installation Using Source Code
```
# update
$ apt-get update
$ apt-get upgrade
$ wget https://bootstrap.pypa.io/2.7/get-pip.py
$ python get-pip.py

$ git clone https://github.com/ctsit/redi.git redi
$ cd redi
$ make && make install

[ctrl-d] # exit container

```

### 5) Commit, tag and save to Dockerhub
```
docker ps -a
docker commit [CONTAINER ID] dominicklemas/redi
docker tag [IMAGE ID] dominicklemas/redi:03_2021
docker login
docker push dominicklemas/redi:03_2021
```

### 6) need to create a dockerfile