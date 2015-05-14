FROM debian:wheezy

MAINTAINER Nakul G Selvaraj <nakulgan@gmail.com>

# add our user and group first to make sure their IDs get assigned consistently, regardless of whatever dependencies get added
RUN groupadd -r redis && useradd -r -g redis redis

RUN apt-get update \
  && apt-get install -y curl \
  && rm -rf /var/lib/apt/lists/*

# grab gosu for easy step-down from root
RUN gpg --keyserver pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4
RUN curl -o /usr/local/bin/gosu -SL "https://github.com/tianon/gosu/releases/download/1.2/gosu-$(dpkg --print-architecture)" \
  && curl -o /usr/local/bin/gosu.asc -SL "https://github.com/tianon/gosu/releases/download/1.2/gosu-$(dpkg --print-architecture).asc" \
  && gpg --verify /usr/local/bin/gosu.asc \
  && rm /usr/local/bin/gosu.asc \
  && chmod +x /usr/local/bin/gosu

ENV REDIS_VERSION 3.0.0
ENV REDIS_DOWNLOAD_URL http://download.redis.io/releases/redis-3.0.0.tar.gz
ENV REDIS_DOWNLOAD_SHA1 c75fd32900187a7c9f9d07c412ea3b3315691c65

# for redis-sentinel see: http://redis.io/topics/sentinel
RUN buildDeps='gcc libc6-dev make' \
  && set -x \
  && apt-get update && apt-get install -y $buildDeps --no-install-recommends \
  && rm -rf /var/lib/apt/lists/* \
  && mkdir -p /usr/src/redis \
  && curl -sSL "$REDIS_DOWNLOAD_URL" -o redis.tar.gz \
  && echo "$REDIS_DOWNLOAD_SHA1 *redis.tar.gz" | sha1sum -c - \
  && tar -xzf redis.tar.gz -C /usr/src/redis --strip-components=1 \
  && rm redis.tar.gz \
  && make -C /usr/src/redis \
  && make -C /usr/src/redis install \
  && apt-get purge -y --auto-remove $buildDeps

EXPOSE 7001
EXPOSE 7002
EXPOSE 7003
EXPOSE 7004
EXPOSE 7005
EXPOSE 7006

RUN mkdir -p /redis-cluster/7000/
RUN mkdir -p /redis-cluster/7001/
RUN mkdir -p /redis-cluster/7002/
RUN mkdir -p /redis-cluster/7003/
RUN mkdir -p /redis-cluster/7004/
RUN mkdir -p /redis-cluster/7005/

RUN echo "port 7000\ncluster-enabled yes\ncluster-config-file /redis-cluster/7000/nodes.conf\ncluster-node-timeout 5000\nappendonly yes" > /redis-cluster/7000/redis.conf
RUN echo "port 7001\ncluster-enabled yes\ncluster-config-file /redis-cluster/7001/nodes.conf\ncluster-node-timeout 5000\nappendonly yes" > /redis-cluster/7001/redis.conf
RUN echo "port 7002\ncluster-enabled yes\ncluster-config-file /redis-cluster/7002/nodes.conf\ncluster-node-timeout 5000\nappendonly yes" > /redis-cluster/7002/redis.conf
RUN echo "port 7003\ncluster-enabled yes\ncluster-config-file /redis-cluster/7003/nodes.conf\ncluster-node-timeout 5000\nappendonly yes" > /redis-cluster/7003/redis.conf
RUN echo "port 7004\ncluster-enabled yes\ncluster-config-file /redis-cluster/7004/nodes.conf\ncluster-node-timeout 5000\nappendonly yes" > /redis-cluster/7004/redis.conf
RUN echo "port 7005\ncluster-enabled yes\ncluster-config-file /redis-cluster/7005/nodes.conf\ncluster-node-timeout 5000\nappendonly yes" > /redis-cluster/7005/redis.conf

RUN apt-get update
RUN apt-get install -y ruby-full htop
RUN gem install redis

