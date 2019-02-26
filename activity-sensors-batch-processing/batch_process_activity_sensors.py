'''
Aggregate and summarize 33 million rows of accelerator and gyroscope
sensor data from smart watches and mobile phones. Output processed data
to HDF files to preserve metadata.
'''
# Join and summarize 33 million rows of smart watch and phone sensor
# with data Pandas using MapReduce paradigm.
# output to HDF5 files

# file streaming. chunk as generator object.
#
# Group and summarize activity 33 million rows of sensor data by activity,
# user, device and sensor type. Concatenate all summarized data into dataframe.
# Output to HDF5 format (binary format for preserve metadata.
#
# Project assumes that that column names are consistent across CSVs. MapReduce
# paradigm/approach to calculate mean, standard deviation and variance.
#
# Acceleration: rate of change of the velocity of an object.
#
# Gyroscope: measure rotational motion
#
# 3-axis: x left/right, y forward/backward, z up/down
#
# pandas, sensor data, aggregation, HDF5, python, multiindex
#
# More than 80% of the processing time is concentrated in grouping and
# aggregating the data frame chunks. __next__, get_chunk, read functions


import os

import numpy as np
import pandas as pd


class SensorInput:
    '''Read CSV data into data frame chunks.'''

    def __init__(self, dir_data, csv_file, column_names, data_types,
                 chunk_size):
        self.dir_data = dir_data
        self.csv_file = csv_file
        self.column_names = column_names
        self.data_types = data_types
        self.chunk_size = chunk_size

    def read_data(self):
        '''Read CSV data into data frame chunks.

        Returns
        -------
        df: pandas.io.parsers.TextFileReader
        '''
        csv_path = os.path.join(self.dir_data, self.csv_file)
        df = pd.read_csv(csv_path, usecols=self.column_names,
                         dtype=self.data_types, chunksize=self.chunk_size)
        return df


class SensorProcess:
    '''Process data frame chunks into aggregated and summarized data.'''

    def __init__(self, agg_funcs, cols_axes, group_by):
        self.agg_funcs = agg_funcs
        self.cols_axes = cols_axes
        self.group_by = group_by

    @staticmethod
    def _reduce_mean(count, mean):
        '''Reduce mapped means.

        Parameters
        ----------
        count : pandas.Series
        mean : pandas.Series

        Returns
        -------
        float
        '''
        return count.multiply(mean).sum() / count.sum()

    def _reduce_variance(self, count, mean, variance):
        '''Reduce mapped variances.

        Parameters
        ----------
        count : pandas.Series
        mean : pandas.Series
        variance : pandas.Series

        Returns
        -------
        reduced_variance : float
        '''
        reduced_mean = self._reduce_mean(count, mean)
        delta = mean.subtract(reduced_mean).pow(2)
        reduced_variance = ((count.multiply(variance).sum() +
                             count.multiply(delta).sum()) /
                            count.sum())
        return reduced_variance

    def _reduce_summary_statistics(self, s, g, cols):
        '''Calculate summary statistics.

        Parameters
        ----------
        s : dict
        g : pandas.DataFrame
            Data frame of grouped data.
        cols : list
            List of summary statistics columns names.

        Returns
        -------
        s : dict
        '''
        count, max_, mean, min_, std, sum_, var = cols
        s[count] = g[count].sum()
        s[sum_] = g[sum_].sum()
        s[mean] = self._reduce_mean(g[count], g[mean])
        s[min_] = g[min_].min()
        s[max_] = g[max_].max()
        s[var] = self._reduce_variance(g[count], g[mean], g[var])
        s[std] = np.sqrt(s[var])
        return s

    def _reduce_groups(self, group, cols_stats):
        '''Apply summary statistics function to subsets of columns.

        The summary statistics columns (e.g. count, sum, etc.) are
        repeated for each x, y and z direction. Apply the summary
        statistics function to each direction subgroup.

        Parameters
        ----------
        group : pandas.DataFrame
            Data frame of grouped data.
        cols_stats : list
            List of data frame column names.

        Returns
        -------
        pandas.Series
        '''
        series = {}

        for i in range(len(self.cols_axes)):
            cols_direction = [col for col in cols_stats
                              if col.startswith(self.cols_axes[i])]
            cols_direction.sort()
            series = self._reduce_summary_statistics(series, group,
                                                     cols_direction)

        index = cols_stats
        return pd.Series(series, index=index)

    def _reduce_chunks(self, df):
        '''Apply reduce functions to grouped data frame.

        Parameters
        ----------
        df : pandas.DataFrame
            Data frame of aggregated data.

        Returns
        -------
        df_reduced : pandas.DataFrame
        '''
        std_col_names = zip(self.cols_axes, ['std'] * len(self.cols_axes))
        cols_std = [''.join(s) for s in std_col_names]
        cols_stats = df.columns.tolist() + cols_std

        df_reduced = (df.groupby(self.group_by)
                      .apply(lambda group: self._reduce_groups(group,
                                                               cols_stats)))
        df_reduced.sort_index(axis='columns', inplace=True)
        df_reduced.drop(self.group_by, axis='columns', inplace=True)
        return df_reduced

    def _aggregate_chunks(self, df):
        '''Group and aggregate TextFileReader chunks.

        Parameters
        ----------
        df : pandas.io.parsers.TextFileReader

        Returns
        -------
        df_agg : pandas.DataFrame
        '''
        intermediate_stats = []

        for chunk in df:
            chunk = (chunk.groupby(self.group_by)
                     .aggregate(self.agg_funcs)
                     .reset_index())
            chunk.columns = ['_'.join(col) if col[1] else col[0] for col
                             in chunk.columns]
            intermediate_stats.append(chunk)

        df_agg = (pd.concat(intermediate_stats, axis='index')
                  .reset_index(drop=True))
        return df_agg

    def process_sensor(self, df):
        '''Run the processing functions.

        Parameters
        ----------
        df : pandas.io.parsers.TextFileReader

        Returns
        -------
        df : pandas.DataFrame
        '''
        df = self._aggregate_chunks(df)
        df = self._reduce_chunks(df)
        return df


class SensorOutput:
    '''Output aggregated and summarized data to HDF to preserve metadata.'''

    def __init__(self, dir_output, output_file_name):
        self.dir_output = dir_output
        self.output_file_name = output_file_name

    def to_hdf(self, df):
        '''Write summarized data frame to HDF.

        Parameters
        ----------
        df : pandas.DataFrame

        Returns
        -------
        None
        '''
        path_output = os.path.join(self.dir_output, self.output_file_name)
        hdf_key = self.output_file_name.split('.')[0]
        df.to_hdf(path_output, key=hdf_key, format='table', mode='w')


def main():
    '''Run processing program.'''

    dir_data = os.path.abspath('data/')
    dir_output = os.path.abspath('output/')
    csv_files = [
        'Phones_accelerometer.csv',
        'Phones_gyroscope.csv',
        'Watch_accelerometer.csv',
        'Watch_gyroscope.csv'
    ]
    hdf_files = [
        'phone_accel.h5',
        'phone_gyro.h5',
        'watch_accel.h5',
        'watch_gyro.h5'
    ]

    column_names = ('x', 'y', 'z', 'User', 'Model', 'Device', 'gt')
    data_types = {
        'x': np.float32,
        'y': np.float32,
        'z': np.float32,
        'User': 'category',
        'Model': 'category',
        'Device': 'category',
        'gt': 'category'
    }
    chunk_size = 100000

    agg_funcs = ['count', 'sum', 'mean', 'min', 'max', 'var']
    group_by = ['gt', 'User', 'Device']
    cols_axes = ('x_', 'y_', 'z_')

    input_output = zip(csv_files, hdf_files)

    for input_file, output_file in input_output:

        sensor_input = SensorInput(dir_data, input_file, column_names,
                                   data_types, chunk_size)
        df = sensor_input.read_data()

        sensor_process = SensorProcess(agg_funcs, cols_axes, group_by)
        df = sensor_process.process_sensor(df)

        sensor_output = SensorOutput(dir_output, output_file)
        sensor_output.to_hdf(df)


if __name__ == '__main__':

    main()
