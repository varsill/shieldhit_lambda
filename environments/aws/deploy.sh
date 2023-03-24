
zip -j shieldhit_deploy.zip converters.py common.py environments/aws/lambda_function.py environments/environment_common.py binaries/shieldhit binaries/convertmc
aws lambda update-function-code --function-name shieldhit_test_python_3_8 --zip-file fileb://shieldhit_deploy.zip
#rm shieldhit_deploy.zip