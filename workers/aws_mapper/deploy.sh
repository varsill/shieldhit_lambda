
zip -j shieldhit_deploy.zip converters.py common.py workers/aws_mapper/lambda_function.py workers/common/mapper.py binaries/shieldhit binaries/convertmc
aws lambda update-function-code --function-name shieldhit_test_python_3_8 --zip-file fileb://shieldhit_deploy.zip
rm shieldhit_deploy.zip