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
    pymchelper

# workaround - the Singularity image has h5py and numpy installed with the proper native dependencies,
# so we don't want to use the dependencies installed as side dependencies to pymchelper in the shieldhit_deploy directory
rm -rf shieldhit_deploy/numpy*
rm -rf shieldhit_deploy/h5py*

(cd shieldhit_deploy/ && zip -r ../shieldhit_deploy.zip .)
./binaries/wsk -i action update shieldHit shieldhit_deploy.zip --kind python:3
rm shieldhit_deploy.zip
rm -r shieldhit_deploy
