FROM ubuntu:24.04
LABEL authors="jrsoares"

RUN apt-get update
RUN apt-get install -y  \
      cmake \
      clang-16 \
      build-essential \
      git \
      autoconf \
      libibverbs-dev \
      ibverbs-providers \
      ibverbs-utils \
      rdma-core \
      libnuma-dev \
      zlib1g-dev \
    libboost-dev \
    libboost-fiber-dev \
    libboost-context-dev \
    libboost-thread-dev \
    libboost-system-dev \
    cmake \
    libgtest-dev \
    curl

WORKDIR /src

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-16 100

WORKDIR /src
RUN mkdir ziplog
WORKDIR ziplog
COPY . .