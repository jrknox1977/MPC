#!/bin/bash
yum -y update && yum -y upgrade
yum -y install python36 python36-devel python36-pip python36-setuptools
pip-3.6 install --upgrade pip
