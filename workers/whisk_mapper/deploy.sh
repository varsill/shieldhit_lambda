#! /bin/bash

mkdir shieldhit_deploy
cp binaries/convertmc.py shieldhit_deploy/convertmc
cp binaries/shieldhit shieldhit_deploy/
cp converters.py shieldhit_deploy/
cp common.py shieldhit_deploy/ 
cp workers/whisk_mapper/__main__.py shieldhit_deploy/
cp workers/common/mapper.py shieldhit_deploy/
cp z_profile_0000.bdo shieldhit_deploy/

python3 -m pip install \
    --platform manylinux2014_x86_64 \
    --target=shieldhit_deploy \
    --implementation cp \
    --python 3.8 \
    --only-binary=:all: --upgrade \
    pymchelper numpy==1.21 h5py==3.1

# python3 -m pip install \
#     --platform manylinux2014_x86_64 \
#     --target=shieldhit_deploy \
#     --implementation cp \
#     --python 3.8 \
#     --only-binary=:all: --upgrade \
#     h5py
#rm -rf shieldhit_deploy/numpy*

(cd shieldhit_deploy/ && zip -r ../shieldhit_deploy.zip .)
./binaries/wsk -i action update shieldHit shieldhit_deploy.zip --kind python:3
rm shieldhit_deploy.zip
rm -r shieldhit_deploy
