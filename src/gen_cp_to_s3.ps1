
$StackName="g1-coj-1-click-ETL"

$ResourcesBucket="g1-coj-resources-bucket"
$FirstTriggerBucket="g1-coj-trigger-bucket-1"
$SecondTriggerBucket="g1-coj-trigger-bucket-2"

$FirstLambdaFunctionName='g1-coj-ET-lambda'
$SecondLambdaFunctionName="g1-coj-L-lambda"

$FirstLambdaPythonFile='lambda_function_1.py'
$SecondLambdaPythonFile='lambda_function_2.py'

$Layer1="pandas-python-3-9.zip"
$Layer2="psycopg2-sqlalchemy-utils.zip"

$DbHost="redshiftcluster-gyryx7hwpsmz.cv7hcrmjdnhd.eu-west-1.redshift.amazonaws.com"
$DbPort="5439"
$DbUser="group01"
$DbPassword="Redshift-deman4-group01"
$DbDatabase="group01_cafe"
$DbTablePrefix="test_"

$CloudFormationFile="gen-cloud-formation.yaml"


$FirstLambdaZipFile=$FirstLambdaPythonFile.Replace(".py",".zip")
$SecondLambdaZipFile=$SecondLambdaPythonFile.Replace(".py",".zip")

$FirstPythonFileName=$FirstLambdaPythonFile.Replace(".py","")
$SecondPythonFileName=$SecondLambdaPythonFile.Replace(".py","")


Compress-Archive $($FirstLambdaPythonFile) $($FirstLambdaZipFile) -Force

Compress-Archive $($SecondLambdaPythonFile) $($SecondLambdaZipFile) -Force

aws s3 mb s3://$($ResourcesBucket)

aws s3api put-public-access-block --bucket $($ResourcesBucket) --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

aws s3 mb s3://$($FirstTriggerBucket)

aws s3api put-public-access-block --bucket $($FirstTriggerBucket) --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

aws s3 mb s3://$($SecondTriggerBucket)

aws s3api put-public-access-block --bucket $($SecondTriggerBucket) --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"



aws s3 cp $($FirstLambdaZipFile) s3://$($ResourcesBucket)
aws s3 cp $($SecondLambdaZipFile) s3://$($ResourcesBucket)

aws s3 cp $($Layer1) s3://$($ResourcesBucket)
aws s3 cp $($Layer2) s3://$($ResourcesBucket)
aws s3 cp products_menu.csv s3://$($ResourcesBucket)

aws s3 cp $($CloudFormationFile) s3://$($ResourcesBucket)


aws cloudformation create-stack `
--stack-name $($StackName) `
--template-url https://$($ResourcesBucket).s3.eu-west-1.amazonaws.com/$($CloudFormationFile) `
--region eu-west-1 `
--parameters `
ParameterKey=ResourcesBucket,ParameterValue=$($ResourcesBucket) `
ParameterKey=FirstTriggerBucket,ParameterValue=$($FirstTriggerBucket) `
ParameterKey=SecondTriggerBucket,ParameterValue=$($SecondTriggerBucket) `
ParameterKey=FirstLambdaFunctionName,ParameterValue=$($FirstLambdaFunctionName) `
ParameterKey=SecondLambdaFunctionName,ParameterValue=$($SecondLambdaFunctionName) `
ParameterKey=FirstPythonFileName,ParameterValue=$($FirstPythonFileName) `
ParameterKey=SecondPythonFileName,ParameterValue=$($SecondPythonFileName) `
ParameterKey=Layer1,ParameterValue=$($Layer1) `
ParameterKey=Layer2,ParameterValue=$($Layer2) `
ParameterKey=DbHost,ParameterValue=$($DbHost) `
ParameterKey=DbPort,ParameterValue=$($DbPort) `
ParameterKey=DbUser,ParameterValue=$($DbUser) `
ParameterKey=DbPassword,ParameterValue=$($DbPassword) `
ParameterKey=DbDatabase,ParameterValue=$($DbDatabase) `
ParameterKey=DbTablePrefix,ParameterValue=$($DbTablePrefix) `
--capabilities CAPABILITY_IAM


