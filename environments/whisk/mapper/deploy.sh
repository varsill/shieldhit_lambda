#! /bin/bash

mkdir shieldhit_deploy
cp binaries/convertmc.py shieldhit_deploy/convertmc
cp converters.py shieldhit_deploy/ 
cp environments/whisk/mapper/__main__.py shieldhit_deploy/
cp environments/shieldhit_executor.py shieldhit_deploy/ 
cp binaries/shieldhit shieldhit_deploy/

#docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip install -r environments/whisk/mapper/requirements.txt"


#zip -r shieldhit_deploy.zip  environments/whisk/mapper/__main__.py  environments/shieldhit_executor.py converters.py binaries/shieldhit convertmc virtualenv/
(cd shieldhit_deploy/ && zip -r ../shieldhit_deploy.zip .)
./binaries/wsk -i action update shieldHit shieldhit_deploy.zip --kind python:3
rm shieldhit_deploy.zip
rm -r shieldhit_deploy
