#!/bin/bash
CONFIG_S3=$1

rm -rf './temp'
mkdir './temp'

echo "----- starting virtualenv -----"
sudo -H pip install virtualenv
virtualenv -p /usr/bin/pypy ./pythonScript/.venv
echo "----- finished virtualenv -----"

echo "----- activating virtualenv and install pip-----"
. ./pythonScript/.venv/bin/activate
pip install -r ./pythonScript/requirements.txt
echo "----- activated virtualenv and installed pip -----"

echo "----- starting app.py with config $CONFIG_S3 -----"
output = pypy ./pythonScript/app.py --config $CONFIG_S3 
echo "----- app.py finished -----"