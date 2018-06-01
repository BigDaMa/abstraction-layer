##Installation

#Setting Up and Using Conda
#For 64 bit machines, run:
wget https://3230d63b5fc54e62148e-c95ac804525aac4b6dba79b00b39d1d3.ssl.cf1.rackcdn.com/Anaconda-2.3.0-Linux-x86_64.sh
bash Anaconda-2.3.0-Linux-x86_64.sh

#using conda 
conda create -n py27Env python=2.7 anaconda

#active 
source activate py27Env

##Install Postgresql

sudo apt-get install postgresql postgresql-contrib

#using postgres
sudo -u postgres psql

# Setup Postgres for Holoclean
# you can create the table or simply use the pg_script_ubuntu.sh in Holoclean folder

CREATE database holo;
CREATE user holocleanuser;
ALTER USER holocleanuser WITH PASSWORD 'abcd1234';
GRANT ALL PRIVILEGES on database holo to holocleanUser ;

#To Connect to the holo database run

\c holo
drop database holo;
create database holo;

#Installing Pytorch
#use the following command if it doesn't work  check  http://pytorch.org/

conda install pytorch torchvision -c pytorch

#Installing Required Packages
pip install -r python-package-requirement.txt

#Install JDK 8

sudo apt-get install openjdk-8-jre

# then you should open your command in Holoclean folder and after this 3 lines you can start ./start_notebook.sh

#source activate py27Env
#source /etc/profile
#source activate py27Env

