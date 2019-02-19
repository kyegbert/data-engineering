'''Transform historical daily NASDAQ stocks price data into summary
statistics using multithreading, multiprocessing and queues.

See the accompanying README.md file for further explanation.
'''


from collections import namedtuple
import csv
import logging
from multiprocessing import Manager, Pool
import queue
import os
import sys
import threading

import numpy as np


logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
logger = logging.getLogger(__name__)

# An attribute-not-found error occurs when Pickle attempts to serialize
# a named tuple from within a class method. Define the named tuple at
# top-level so that Pickle can locate the named tuple when serializing.
# Pickle is utilized in the multiprocessing Pool module.
StockStats = namedtuple('StockStats', 'symbol count mean min max std')


class StocksInput:
    '''Read NASDAQ stock data from multiple CSV files into an input
    queue.

    Parameters
    ----------
    path_nasdaq : str
        Path to CSV of NASDAQ stock symbols.
    column_names : tuple
        Tuple of standardized column names in NASDAQ stocks CSVs.
    stop_token : str
        Sentinel value indicating end of queue.
    '''

    def __init__(self, path_nasdaq, column_names, stop_token):
        self.path_nasdaq = path_nasdaq
        self.column_names = column_names
        self.stop_token = stop_token

    @staticmethod
    def _from_csv(path, row_slice, process_csv_data, **kwargs):
        '''Read data from CSV file.

        Apply custom processing function while data is read from CSV.

        Parameters
        ----------
        path : str
            Path to stock CSV.
        row_slice : slice
            Slice list of row values returned by CSV reader, if needed.
        process_csv_data : method
            Customized method to process row data while it is read
            from CSV.
        **kwargs : kwargs
            Keyword arguments for customized processing function.

        Returns
        -------
        None
        '''
        try:
            if os.stat(path).st_size > 0:
                with open(path, 'r') as f:
                    reader = csv.reader(f, delimiter=',')
                    next(reader)
                    for row in reader:
                        if ''.join(row).strip():
                            process_csv_data(row[row_slice], **kwargs)
        except OSError as e:
            logger.exception(e, exc_info=True)

    def read_exchange_symbols(self):
        '''Read NASDAQ stock symbols column from CSV into list.'''
        symbols = []
        self._from_csv(self.path_nasdaq, slice(0, 1), symbols.extend)
        symbols = [sym.lower() for sym in symbols]
        return symbols

    def _group_stock_data(self, row, grouped_stock):
        '''Add row of CSV data to dictionary.

         Dictionary key is stock symbol and value is columnar values
         for that symbol.

        Parameters
        ----------
        row : list
            List of columnar data from a single row in the CSV.
        grouped_stock : list
            List contains an identifying stock symbol and its
            corresponding CSV columnar data organized in a dictionary.

        Returns
        -------
        grouped_stock : list
            List contains an identifying stock symbol and its
            corresponding CSV columnar data organized in a dictionary.
        '''
        row = row[:-1]
        for item in zip(self.column_names, row):
            col_values = item[0]  # list of columnar data
            col_value = item[1]
            grouped_stock[1][col_values].append(col_value)
        return grouped_stock

    def _read_stock_data(self, path):
        '''Read and group stocks data from CSV files.

        The stock data needs a small amount of grouping at this
        threading stage to link a stock symbol to its corresponding
        CSV data. Empty CSVs are ignored.

        Parameters
        ----------
        path : str
            Path to stock CSV file.

        Returns
        -------
        grouped_stock : list
            List contains an identifying stock symbol and its
            corresponding CSV columnar data organized in a dictionary.
        '''
        if os.stat(path).st_size > 0:
            base = os.path.basename(path)
            file_name = base.split('.')[0]

            stock_data = {key: [] for key in self.column_names}
            grouped_stock = [file_name, stock_data]
            kwargs = {'grouped_stock': grouped_stock}

            self._from_csv(path, slice(None), self._group_stock_data, **kwargs)
        else:
            grouped_stock = None
        return grouped_stock

    def input_queue(self, queue_in, queue_out):
        '''Put grouped stocks data into processing queue.

        Parameters
        ----------
        queue_in : queue
            Queue of paths to stocks CSV files.
        queue_out : queue
            Queue of lists; each list consists of an identifying stock
            symbol and a dictionary of corresponding CSV columnar data.

        Returns
        -------
        None
        '''
        while True:
            item = queue_in.get()
            if item == self.stop_token:
                break
            grouped_stock = self._read_stock_data(item)
            if grouped_stock:
                queue_out.put(grouped_stock)
            queue_in.task_done()


class StocksProcess:
    '''Transform columnar stock data to summary statistics.

    Parameters
    ----------
    multi_queues : dict
        Dictionary where keys are stocks CSV columns and values are
        output queues.
    stop_token : str
        Sentinel value indicating end of queue.
    column_names : tuple
        Tuple of column names in stocks CSV files.
    '''

    def __init__(self, multi_queues, stop_token, column_names):
        self.multi_queues = multi_queues
        self.stop_token = stop_token
        self.column_names = column_names

    @staticmethod
    def _to_numeric(item):
        '''Convert string to int or float or leave as str.'''
        try:
            return int(item)
        except ValueError:
            pass
        try:
            return float(item)
        except ValueError:
            pass
        return item

    def _summary_statistics(self, symbol, col_values):
        '''Calculate summary statistics for a single CSV column.

        Parameters
        ----------
        symbol : str
            Identifying stock symbol.
        col_values : list
            List of int or float data from a particular column in a CSV
            file for a individual stock.

        Returns
        -------
        StockStats : named tuple
            Named tuple of summary statistics for a column of values
            for an individual stock.
        '''
        col_values = [self._to_numeric(col) for col in col_values]
        count = len(col_values)
        mean = np.mean(col_values)
        minimum = np.min(col_values)
        maximum = np.max(col_values)
        std = np.std(col_values)
        return StockStats(symbol, count, mean, minimum, maximum, std)

    def _summarize_stock(self, col, item):
        '''Summarize stock by CSV column.

        Parameters
        ----------
        col : str
            CSV column name.
        item : list of stock symbol and dictionary of CSV columnar
            values.

        Returns
        -------
        stock_summary : named tuple
            Named tuple of summary statistics for an individual stock.
        '''
        symbol = item[0]
        column_values = item[1]
        stock_summary = self._summary_statistics(symbol, column_values[col])
        return stock_summary

    def _to_queue(self, item):
        '''Put stock summary statistics in specific output queue.

        Parameters
        ----------
        item : list
            List of stock symbol and dictionary of CSV columnar
            values.

        Returns
        -------
        None
        '''
        for col in self.column_names[1:]:
            stock_summary = self._summarize_stock(col, item)
            self.multi_queues[col].put(stock_summary)

    def process_queue(self, queue_process):
        '''Get grouped stock data for transfer to output queues.

        Parameters
        ----------
        queue_process : queue
            Queue of lists of stock symbols and dictionaries of CSV
            columnar values.

        Returns
        -------
        None
        '''
        while True:
            item = queue_process.get()
            if item == self.stop_token:
                break
            else:
                self._to_queue(item)


class StocksOutput:
    '''Write transformed CSV data to CSV output files.

    Parameters
    ----------
    stop_token : str
        Sentinel value indicating end of queue.
    dir_output : str
        Path to directory where output files will be written.
    '''

    def __init__(self, stop_token, dir_output):
        self.stop_token = stop_token
        self.dir_output = dir_output

    def _write_stock_data(self, queue_output, writer):
        '''Consume output queue items for writing to CSV.

        Parameters
        ----------
        queue_output : queue
            Queue of summarized stock data.
        writer : CSV writer object

        Returns
        -------
        None
        '''
        while True:
            item = queue_output.get()
            if item == self.stop_token:
                break
            else:
                writer.writerow(item)
            queue_output.task_done()

    def to_csv(self, file_name, queue_output):
        '''Write output queue to CSV file.

        Write output queue of summarized data for a specific stock
        feature (i.e., open, high, low, close, volume) to CSV file.

        Parameters
        ----------
        file_name : str
            Column name from stocks CSV files.
        queue_output : queue
            Output queue of named tuples.

        Returns
        -------
        None
        '''
        header = StockStats._fields
        file_name = ''.join(['nasdaq_', file_name, '.csv'])
        path = os.path.join(self.dir_output, file_name)

        try:
            with open(path, 'a+') as f:
                writer = csv.writer(f, )
                if f.tell() == 0:
                    writer.writerow(header)
                self._write_stock_data(queue_output, writer)
        except OSError as e:
            logger.exception(e, exc_info=True)


def main():
    '''Run processing program.'''

    column_names = ('date', 'open', 'high', 'low', 'close', 'volume')

    dir_data = os.path.abspath('data/')
    dir_stocks = os.path.abspath('data/stocks/')
    dir_output = os.path.abspath('output/')
    path_nasdaq = os.path.join(dir_data, 'nasdaq_companies.csv')
    file_names = os.listdir(dir_stocks)

    stop_token = 'STOP'

    stocks_input = StocksInput(path_nasdaq, column_names, stop_token)
    nasdaq_symbols = stocks_input.read_exchange_symbols()
    paths = [os.path.join(dir_stocks, f) for f in file_names
             if f.split('.')[0] in nasdaq_symbols]

    manager = Manager()
    q_input = queue.Queue()
    q_process = manager.Queue()
    output_queues = {key: manager.Queue() for key in column_names
                     if key != 'date'}

    num_threads = 3
    threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=stocks_input.input_queue,
                                  args=(q_input, q_process))
        thread.start()
        threads.append(thread)

    for path in paths:
        q_input.put(path)

    for _ in range(num_threads):
        q_input.put(stop_token)
    for thread in threads:
        thread.join()

    stocks_process = StocksProcess(output_queues, stop_token, column_names)

    pool = Pool()
    pool.imap(stocks_process.process_queue, (q_process,))
    q_process.put(stop_token)

    pool.close()
    pool.join()

    output_queues = stocks_process.multi_queues
    for value in output_queues.values():
        value.put(stop_token)

    stocks_output = StocksOutput(stop_token, dir_output)

    for file_name, q_output in output_queues.items():
        thread = threading.Thread(target=stocks_output.to_csv,
                                  args=(file_name, q_output))
        thread.start()
        thread.join()


if __name__ == "__main__":

    main()
