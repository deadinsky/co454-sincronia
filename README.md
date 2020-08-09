# co454-sincronia

This code was created using Java 1.11 on a Linux setup but should work with Java 1.8.

There are three ways to build these project components:

* If you would like the intercommunication setup, run ./build.sh . Note that you will need Apache Thrift 0.13.0 and it will run across three different processes.
* If you would like to run this in single process mode to more easily analyze the Sincronia implementation, run ./build_local.sh
* If you would like to run the brute force solver made to calculate the optimal schedule's average weighted coflow completion time, run ./build_solver.sh

Each of the build scripts run the exact same way. They will try to detect your Java Home directory and build the program for you.
It should cover most Linux Java installations, but if it does not, you may need to edit the script to include your Java Home.

Once you've run a build script, it will give you the exact command line instruction you need.
Feel free to use any schedule created in the style of [the workload generator](https://github.com/sincronia-coflow/workload-generator) as the input text file.
