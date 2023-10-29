# SHIELDHIT Lambda 
SHIELDHIT Lambda is a tool that allows to perform particle transport simulation in highly distributed environments, such as Function-as-a-Service platforms.
The working principle of the SHIELDHIT Lambda is running multiple instances of [SHIELDHIT-12A particle transport simulator](https://shieldhit.org/) in parallel, extracting the results of each instance run with the use of `pymchelper`(https://github.com/DataMedSci/pymchelper) and aggregating the final results. Each of these steps can be performed either on the local machine or with the use of FaaS platform.

In the repository you can find functionalites to:
* deploy and run the SHIELDHIT-12A particle transport simulator in highly distributed environments, on different Function-as-a-Service platforms such as:
  * [AWS Lambda](https://aws.amazon.com/lambda/)
  * [HPCWhisk](https://arxiv.org/abs/2211.00717) - a fork of [OpenWhisk](https://openwhisk.apache.org/) adjusted to be run on HPC clusters
* meassure performance and quality of the results of different test scenarios of running a particle transport simulator
* analyze the results of the tests and visualize them


## Repository structure
The repository consists of the following directories:
* `binaries/` - where the SHIELDHIT-12A and pymchelper binaries should be put
* `datatypes/` - with modules providing a convenient API to load and write data
* `dumps/` - with artifacts being the .dump files with test results of running tests
* `environments/` - with FaaS platform-specific code and scripts to deploy the programs on different FaaS platforms
* `input/` - with SHIELDHIT-12A specific .dat files describing the simulation input
* `launchers/` - with description of different simulation scenarios
* `metrics/` - with functions, commandline scripts and Jupyter Notebook file allowing to process the result .dump files of tests and visualize them
* `workers/` - with distributed simulator implementation for different working environemnts (local, AWS Lambda, HPC Whisk)

## Installation and initial setup
First, you need to put the following files (with appropriate names) into the `binary/` directory: 
* `SHIELDHIT-12A` binary ([you can get it here](https://shieldhit.org/index.php?id=download)) as `shieldhit`
* `pymchelper` binary ([you can get it here](https://github.com/DataMedSci/pymchelper)) as `pymchelper`
* `wsk` binary ([you can get it here](https://github.com/apache/openwhisk-cli#downloading-released-binaries))

Then you need to install AWS command line interface (for manual how to do it, click [here](https://aws.amazon.com/cli/)).

After completing that you need to create your AWS Lambda instance and your HPCWhisk action.
As a follow-up, setup the following environmental variables:
* `AWS_LAMBDA_URL` - the URL of your AWS Lambda instanc whhere the remote function will be executed
* `WHISK_ACTION_URL` - the URL of your HPCWhisk action where the remote function will be executed

Then you need to deploy the remote functions on the FaaS platforms. Type:
```
source ./environments/aws/deploy.sh # for deploying on AWS Lambda
source ./environments/whisk/deploy.sh # for deploying on HPCWhisk
```

## Running the tests 
The tests can be run with the use of `./test_runner.py` script.
At the beggining of the `./test_runner.py` script there is a bunch of global variables that allow you configure the test. The semantics behind these variables is following:
* LAUNCH_NAME - the name of the launcher to be used in the test. The `launchers/` directory contains modules with different launchers implementation. Each launcher runs within three steps:
  * `simulate` - running the `SHIELDHIT-12A` simulation
  * `extract` - running the `pymchelper` to extract the HDF representation of results in form of a matrix
  * `reduce` - running the script to calculate the mean of values from the result matrices
Each launcher is described as a string of form "<first step>__<second step>__<third step>", where
"first step", "second step" and "third step" describe how the particular steps of the launcher should be performed ("local" stands for running on the machine where the script is run, while "remote" stands for running as a remote function on a FaaS platform)
* METRICS_RESULT_BASE_PATH - the directory where the .dump file with test results will be created
* TEST_RUNNER_POSTFIX - a string that will be appended to the .dump file with the test results
* HOW_MANY_TRIES - a positive integer describing how many times a particular test case should be run
* TEST_CASES - a map describing the test cases. During the test, all the test cases being the cartesian product of the values described in this variable will be launched. You can configure the following variables in this map:
  * "how_many_workers" - number of functions that will run the `simulate` step of the launcher
  * "how_many_samples" - number of particle samples to be simulated
  * "faas_environment" - "aws" or "whisk", applicable for all LAUNCH_NAME values except "local__local__local" 
  * "reduce_when" - a number describing results from how many `simulate` steps should be initially reduced together, applicable only for the following LAUNCH_NAME values: "remoteA__remoteB_and_local__persistent" and "remoteA__remoteB__remoteB_and_local"

Once you are happy with your test description, you can launch the test with the following command:
```
python3 ./test_runner.py
```
## Analyzing the test's results
As a result of tests run, the files with `.dump` extension should appear in the directory pointed by `METRICS_RESULT_BASE_PATH`.
The file contains the binary representation (serialized with the use of `picke` Python library) of a map 
containing metrics measured during the test.
### Jupiter notebook
You can use the [Jupyter Notebook](https://jupyter.org/) to open `metrics/plots.ibynb` and see how an exemplary
processing of the .dump files might look like.  
### Command line script for results analysis
You can also use the `metrics/analyze.py` command line script and use it to produce different types of plots.
```
python3 metrics/analyze.py -h
...
usage: analize.py [-h] [-g GROUP_BY_PARAM] [-o OUTPUT] [-l LIMIT] [-t TITLE]
                  input
                  {histogram,success_ratio,execution_and_request,execution_to_request_ratio,phases_makespan,distribution,mse,psnr,cumulative_workers_times,speedup}
                  ...

positional arguments:
  input

optional arguments:
  -h, --help            show this help message and exit
  -g GROUP_BY_PARAM, --group_by_param GROUP_BY_PARAM
  -o OUTPUT, --output OUTPUT
  -l LIMIT, --limit LIMIT
  -t TITLE, --title TITLE

operation:
  {histogram,success_ratio,execution_and_request,execution_to_request_ratio,phases_makespan,distribution,mse,psnr,cumulative_workers_times,speedup}
```