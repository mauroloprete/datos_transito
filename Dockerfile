FROM rocker/r-ver:4.1.1

RUN apt-get update && apt-get install -y --no-install-recommends \
	libpng16-16 \
	curl \ 
	libcurl4-openssl-dev \
	libssl-dev \ 
	libxml2-dev \
	libmysqlclient-dev \
	libpq-dev \
	libcairo2-dev \
	libx11-dev 
# Dependencias de R

RUN install2.r --error \
	--deps TRUE \
	DBI \
	RPostgreSQL \
	data.table \
    here 

WORKDIR /home/docker/


COPY R/ ./R
COPY . .
COPY .Renviron ./.Renviron