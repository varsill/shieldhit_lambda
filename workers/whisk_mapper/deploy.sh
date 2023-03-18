#! /bin/bash

mkdir shieldhit_deploy
cp binaries/convertmc.py shieldhit_deploy/convertmc
cp binaries/shieldhit shieldhit_deploy/
cp converters.py shieldhit_deploy/ 
cp workers/whisk_mapper/__main__.py shieldhit_deploy/
cp workers/common/mapper.py shieldhit_deploy/ 

(cd shieldhit_deploy/ && zip -r ../shieldhit_deploy.zip .)
./binaries/wsk -i action update shieldHit shieldhit_deploy.zip --kind python:3
rm shieldhit_deploy.zip
rm -r shieldhit_deploy
