# Data Engineering Portfolio by Kim Y. Egbert

## Description

This repo contains data engineering projects in Python. Each of the repo subdirectories contain a stand-alone data engineering project.

## Projects

### Multithreading, Multiprocessing and Queues

[Summarizing 5.4 Million Rows of NASDAQ Stock Data with Multithreading, Multiprocessing and Queues](/stocks-multithreading-multiprocessing)  
This module ingests 5.4 million rows of daily historical NASDAQ stock price data, processes the data into summary statistics (e.g. mean, standard deviation, etc.) and outputs the processed data into a series of CSV files. Multithreading, multiprocessing, queues, cProfile and yappi are utilized to reduce the processing runtime on a single 3.8 GHz Intel Core i5 with 4 cores by 50% when compared to non-concurrent processing methods. 

### Batch Processing and Aggregation

[Batch Processing and Aggregating 33 Million Rows of Activity Sensor Data](/activity-sensors-batch-processing)  
This module reads 33 million rows of accelerometer and gyroscope sensor data from CSVs, batch processes the data into descriptive summary statistics and outputs the data to HDF files to preserve metadata. The data is processed using Pandas TextFileReader chunks, grouped using a multi-index and summarized with descriptive statistics. The statistics are generated, in part, by a MapReduce paradigm that calculates the mean, standard deviation and variance across chunks.

## License

MIT License. Copyright (c) 2019 Kim Y. Egbert, except where otherwise noted in additional License files.
