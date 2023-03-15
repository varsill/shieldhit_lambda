# mkdir shieldhit_deploy
# cp converters.py shieldhit_deploy/ 
# cp environments/aws/mapper/lambda_function.py shieldhit_deploy/
# cp environments/shieldhit_executor.py shieldhit_deploy/ 
# cp binaries/shieldhit shieldhit_deploy/
# cp binaries/convertmc shieldhit_deploy/

# python3 -m pip install \
#     --platform manylinux2014_x86_64 \
#     --target=shieldhit_deploy \
#     --implementation cp \
#     --python 3.8 \
#     --only-binary=:all: --upgrade \
#     pymchelper[full]

# (cd shieldhit_deploy/ && zip -r ../shieldhit_deploy.zip .)
zip -j shieldhit_deploy.zip converters.py environments/aws/mapper/lambda_function.py environments/shieldhit_executor.py binaries/shieldhit binaries/convertmc
aws lambda update-function-code --function-name shieldhit_test_python_3_8 --zip-file fileb://shieldhit_deploy.zip
rm shieldhit_deploy.zip
# rm -r shieldhit_deploy/