# Batch Processing and Aggregating 33 Million Rows of Activity Sensor Data

## Project Description

This module reads 33 million rows of sensor data from CSVs, batch processes the data into descriptive summary statistics and outputs the data to HDF files. The data set consists of smart watch and mobile phone accelerometer and gyroscope sensor data which was recorded while study participants engaged in a number of activities: biking, sitting, standing, walking, stairs up and stairs down. The processing section of the module utilizes Pandas TextFileReader chunks to process the data in batches. The data is grouped by activity, user and device and then summarized with descriptive statistics. The statistics are generated, in part, by a MapReduce paradigm that calculates the mean, standard deviation and variance across chunks. The summarized data is output to HDF files to preserve metadata.

## Methods Used

* Batch processing
* MapReduce paradigm

## Getting Started 

### Download Data

1. Heterogeneity Activity Recognition Data Set in CSV format. This data set contains accelerometer and gyroscope sensor readings from smart watches and mobile phones generated by study participants performing a number of activities: biking, sitting, standing, walking, stair up and stair down. Download the data at [https://archive.ics.uci.edu/ml/datasets/Heterogeneity+Activity+Recognition](https://archive.ics.uci.edu/ml/datasets/Heterogeneity+Activity+Recognition)
    1. Unzip the CSV files in the `data/` directory.

### Requirements

1. Python 3.6
2. Install project package and module requirements using `pip install -r requirements.txt`.