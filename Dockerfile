# To enable ssh & remote debugging on app service change the base image to the one below
# FROM mcr.microsoft.com/azure-functions/python:4-python3.9-appservice

# FROM mcr.microsoft.com/azure-functions/python:4-python3.9

# ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
#     AzureFunctionsJobHost__Logging__Console__IsEnabled=true

# COPY requirements.txt /
# RUN pip install -r /requirements.txt

# COPY . /home/site/wwwroot



FROM python:3.9.16-bullseye

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN apt-get install -y unixodbc-dev
# optional: kerberos library for debian-slim distributions
# sudo apt-get install -y libgssapi-krb5-2

RUN pip install --upgrade pip setuptools wheel

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

RUN apt-get install wget
RUN wget https://storage.googleapis.com/irisx-nextscm-com/jdk-8u202-linux-x64.tar.gz
RUN tar xvzf jdk-8u202-linux-x64.tar.gz
ENV PATH="${PATH}:/jdk1.8.0_202/bin"

COPY . .
