#! /bin/bash
zip -j shieldhit_deploy.zip  environments/whisk/mapper/__main__.py environments/shieldhit_executor.py converters.py binaries/shieldhit
./binaries/wsk -i action update shieldHit shieldhit_deploy.zip --kind python:3
