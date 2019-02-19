# Data Engineering Portfolio by Kim Y. Egbert

## Description

This repo contains data engineering projects in Python. Each of the repo subdirectories contain a stand-alone data engineering project.

## Projects

### Multithreading, Multiprocessing and Queues

[Summarizing 5.4 Million Rows of NASDAQ Stock Data with Multithreading, Multiprocessing and Queues](/stocks-multithreading-multiprocessing)  
This module ingests 5.4 million rows of daily historical NASDAQ stock price data, processes the data into summary statistics (e.g. mean, standard deviation, etc.) and outputs the processed data into to a series of CSV files. Multithreading, multiprocessing and queues are utilized to reduce the processing runtime on a single 3.8 GHz Intel Core i5 with 4 cores by 50% when compared to non-concurrent processing methods. 

## License

MIT License. Copyright (c) 2019 Kim Y. Egbert, except where otherwise noted in additional License files.