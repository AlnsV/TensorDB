# File System Storage

First version of a file system storage based in xarray.

This project can be seen as a database of files.

## General Structure

There is a principal class that handle the files called FilesStore,
basically it has some general methods as append, update, etc, but
this class is only an interface that was designed to be inherited, extended
and used for saving the settings of every file and internally use another
class to handle every file (this class can be personalized).
The default class to handle files is PartitionsStore, which
is a handler for partitioned netcdf4 files (it could support others format).

## Expected use

The principal use of this library is for BITACORE and probably can be used
in future projects

## Why was created this library?

This library born from the necessity of read the historical prices of
some assets to do backtests and calculate metrics that require matrix
multiplications, basically handle homogenous data.
Normally the data weight more than 1GB and takes to relational
databases more than 1 minute to read all the data and
then more than 30 seconds to transform it into a DataFrame or an array
to make matrix multiplications (This process require load more
than 1GB in memory), and those times are calculated reading the
whole data in one go, if the data is read date by date it takes
even more time.

## Why xarray?

I think that this library is one of the must powerful tools in python.
It provides a set of tool that facility the handle of arrays saved in files.
It extends dask to allow the use of labels, so It's amazing. I think
that documentation explains very well why we must use this library

http://xarray.pydata.org/en/stable/why-xarray.html

## Why netcdf4 file formats are the default format?

1) This format is wonderful for reading, which is the main issue that this
   library attack

2) It is the default format for xarray.
   They allow read multiple files as a unique array.
   They use dask in background

3) It allows modifications of the files without rewrite the whole file and
   in this library we propose a class to handle the data as partitioned files
   to reduce the number of rewrite after appending data