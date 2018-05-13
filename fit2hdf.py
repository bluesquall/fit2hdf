#!/usr/bin/env python2

import os
import logging
import fitparse # only tested for py2
import h5py

logging.basicConfig(level=logging.INFO)

# TODO:
# see: https://github.com/dtcooper/python-fitparse/issues/3


def convert(fitfile, hdffile=None):
    if hdffile is None:
        hdffile = '{}.hdf5'.format(os.path.splitext(fitfile)[0])
    elif hdffile == '-':
        raise NotImplementedError

    with fitparse.FitFile(fitfile) as f:
        records = list(f.get_messages('record'))
    N = len(records)

    logging.info('opening hdf5 file: {}'.format(hdffile))
    #TODO# with ... as ...
    hf = h5py.File(hdffile, 'w')
    logging.debug('{0.filename} opened in mode {0.mode}'.format(hf))
    for i, record in enumerate(records):
        logging.debug('record {0}: {1.name}, {1.type}'.format(i, record))
        for j, field in enumerate(record):
            logging.debug('record {0}, field {1}: {2.name}, {2.type.name}'.format(i, j, field))
            try:
                hf[field.name][i] = field.value
            except IOError: # Can't prepare for writing data (no appropriate function for conversion path)
                if field.name == "timestamp":# or hf[field.name].attrs['unit'] == 'half-percent':
                    hf[field.name][i] = field.raw_value
                elif field.value is None:
                    logging.warning('{0.name} value is None'.format(field)) 
                else:
                    logging.warning('IOError (conversion): {0.name}, {0.value}'.format(field))
            except KeyError: # field hasn't been added as a dataset yet
                #TODO# if N>0, issue a warning
                #TODO# stub the logic below out into a separate method
                if field.name == 'activity_type':
                    logging.debug('activity type: {0}'.format(field.value))
#                    hf.attrs.create('activity_type', field.value) # might become a problem for multi-sport...
                else:
                    try:
                        dtype = field.type.base_type.name
                    except AttributeError:
                        dtype = field.type.name

                    if dtype.startswith('sint'):
                        dtype = dtype[1:]
#                    elif dtype == 'enum':
#                        dtype = 'uint32'
                    # fitparse still yields datatype of uint8 for several of the fields that contain float data, so maybe it would be more friendly to define explicit conversions for every field name, rather than trying to autodetect...

                    try:
                        hf.create_dataset(field.name, (N,), dtype=dtype,
                                compression='gzip', compression_opts=9)
                    except TypeError, te:
                        logging.warning('not sure what do with type: {1} for field {0.name}'.format(field, dtype))
                        raise(te)

                    logging.warning(field.units)

                    unit = field.units
                    if unit is None:
                        unit = 'None'
                    elif unit == 'percent':
                        unit = 'half-percent'

                    hf[field.name].attrs.create('unit', unit)

                    if field.name == 'timestamp':
                        hf[field.name][i] = field.raw_value
                        hf[field.name].attrs.create('unit', 'seconds since 1989-12-31 00:00:00 UTC')
                        #TODO# consider revising to unix epoch
                    else:
                        logging.debug("{0.name} = {0.value}".format(field))
                        if field.value is not None: #TODO# might be better to use a dtype that supports NaN
                            hf[field.name][i] = field.value

                    logging.info('added {0.name} to dataset using type of {1} and units of {0.units}'.format(field, dtype))

    hf.close()


def main(fitfile, hdffile = None):
    convert(fitfile, hdffile)
    #TODO# set creation time to match original file, modification time to current


if __name__ == "__main__":
    import argparse
    # instantiate parser
    parser = argparse.ArgumentParser(description='convert FIT to HDF5')
    # add positional arguments
    parser.add_argument('fitfile', help='the FIT file to convert')
    # add option to strip or convert semicircles to lat/lon
    # actually parse the arguments
    args = parser.parse_args()
    # call the main method to do something interesting
    main(**args.__dict__) #TODO more pythonic?
