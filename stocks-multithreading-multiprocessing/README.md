# Summarizing 5.4 Million Rows of NASDAQ Stock Data with Multithreading, Multiprocessing and Queues

## Project Description

This module ingests 5.4 million rows of daily historical NASDAQ stock price data, processes the data into summary statistics (e.g. mean, standard deviation, etc.) and outputs the processed data into to a series of CSV files. Multithreading, multiprocessing and queues are utilized to reduce the processing runtime on a single 3.8 GHz Intel Core i5 with 4 cores by 50% when compared to non-concurrent processing methods. 

### Input

**Stocks**

The stocks dataset consists of over 7,000 CSV files containing NYSE, NASDAQ, and NYSE MKT historical stock prices. CSVs contain daily observations on opening, high, low and closing stock prices along with stock volume for a particular stock symbol. The date range for the dataset covers January 1970 into November 2017, though not all files in the dataset include observations for the entire date range. The CSV column names (Date, Open, High, Low, Close, Volume, OpenInt) are consistent across all of the CSV files. Some CSV files are empty with no header or observations.

**NASDAQ**

The NASDAQ dataset is a CSV containing stock symbols traded on the NASDAQ market. The stock symbols are utilized to filter out any non-NASDAQ stocks from the module.

### Processing

The stocks data is passes between grouping and summarizing tasks via a series of queues. The queues are processed with multithreading or multiprocessing, depending on whether the task at hand is I/O or CPU intensive.

### Output

The processed data is output to a series of CSV files. Each CSV file represents a column in the stocks input dataset: opening price, high price, low price, closing price, and volume. Each row in a CSV consists of a stock symbol and the summary statistics for that stock and column. For example, the summary statistics of opening prices for all stocks are aggregated in the `nasdaq_open.csv` output file. Each CSV takes the following form of columns and rows:

|symbol|count|mean|min|max|std|
|:---|:---|:---|:---|:---|:---|
|aame|2926|2.7998584688995214|0.44273|5.1975|1.075892804116122|
|aal|989|41.15024974721941|23.698|54.333|6.341739797529356|
|...|...|...|...|...|...|

## Methods Used
* Multithreading
* Multiprocessing
* Queues

## Getting Started

### Download Data

1. Historical daily prices and volumes of all U.S. stocks ranging from 1970 through 1997 in CSV format. Download this data in CSV format from Kaggle at [https://www.kaggle.com/borismarjanovic/price-volume-data-for-all-us-stocks-etfs](https://www.kaggle.com/borismarjanovic/price-volume-data-for-all-us-stocks-etfs) (Note: a Kaggle account is required to download this data).
    1. Unzip the stocks CSV files into the `data/stocks/` directory.
3. Symbols of stocks traded on the NASDAQ exchange. Download the data in CSV format from NASDAQ at [https://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download](https://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download).
    2. Rename the NASDAQ CSV `nasdaq_companies.csv` and place the CSV file in the `data/` directory. 

### Requirements

1. Python 3.6
2. Install project package and module requirements using `pip install -r requirements.txt`.
 
