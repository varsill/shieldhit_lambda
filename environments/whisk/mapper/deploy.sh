#! /bin/bash
cp binaries/convertmc.py convertmc

#docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip install -r environments/whisk/mapper/requirements.txt"

zip shieldhit_deploy.zip  environments/whisk/mapper/__main__.py  environments/shieldhit_executor.py converters.py binaries/shieldhit convertmc 
rm convertmc
./binaries/wsk -i action update shieldHit shieldhit_deploy.zip --kind python:3
#rm shieldhit_deploy.zip
