# co454-sincronia

Implements the sincronia coflow algorithm found at [https://github.com/sincronia-coflow/tech-report/blob/master/sincronia-techreport.pdf](https://github.com/sincronia-coflow/tech-report/blob/master/sincronia-techreport.pdf)

Credit to Christopher Zhu for the analysis sections in the final reports.

This code was created using Java 1.11 on a Linux setup but should work with Java 1.8.

There are four ways to build these project components:

* If you would like the intercommunication setup, run ./build.sh . Note that you will need Apache Thrift 0.13.0 and it will run across three different processes.
* If you would like to run this in single process mode to more easily analyze the Sincronia implementation, run ./build_local.sh
* If you would like to run the brute force solver (iterative) made to calculate the optimal schedule's average weighted coflow completion time, run ./build_solver_iterative.sh
* If you would like to run the brute force solver (recursive) made to calculate the optimal schedule's average weighted coflow completion time, run ./build_solver_recursive.sh

Each of the build scripts run the exact same way. They will try to detect your Java Home directory and build the program for you.
It should cover most Linux Java installations, but if it does not, you may need to edit the script to include your Java Home.

Once you've run a build script, it will give you the exact command line instruction you need.
Feel free to use any schedule created in the style of [the workload generator](https://github.com/sincronia-coflow/workload-generator) as the input text file.
