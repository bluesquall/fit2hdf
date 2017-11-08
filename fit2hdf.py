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
                if field.name == "timestamp":
                    hf[field.name][i] = field.raw_value
                else:
                    logging.warning('IOError (conversion): {0.name}, {0.value}'.format(field))
            except KeyError: # field hasn't been added as a dataset yet
                #TODO# if N>0, issue a warning
                try:
                    hf.create_dataset(field.name, (N,),
                            dtype=field.type.name,
                            compression='gzip', compression_opts=9)
                    try:
                        hf[field.name].attrs.create('unit', field.units)
                    except TypeError as e:
                        if field.units is None:
                            hf[field.name].attrs.create('unit', 'None')
                        else:
                            raise e
                    hf[field.name][i] = field.value
                    logging.info('added {0.name} to dataset using type of {0.type.name} and units of {0.units}'.format(field))
                except TypeError as e: # there's probably a better way to intercept activity_type
                    if field.type.name == "date_time":
                        hf.create_dataset(field.name, (N,),
                                dtype='uint32',
                                compression='gzip', compression_opts=9)
                        hf[field.name].attrs.create('unit', 'seconds since 1989-12-31 00:00:00 UTC')
                        hf[field.name][i] = field.raw_value
                        #TODO# consider revising to unix epoch
                        logging.info('added {0.name} to dataset using type of uint32 and units of {1}'.format(field,hf[field.name].attrs.get('unit')))
                    elif field.type.name.startswith("sint"):
                        hf.create_dataset(field.name, (N,),
                                dtype=field.type.name[1:],
                                compression='gzip', compression_opts=9)
                        hf[field.name].attrs.create('unit', field.units)
                        hf[field.name][i] = field.value
                        logging.info('added {0.name} to dataset using type of int8 and units of {0.units}'.format(field))
                    elif field.type.name == "activity_type":
                        hf.attrs.create('activity_type', field.value)
                    else:
                        logging.warning('not sure what do with type: {0.type.name} for field {0.name}'.format(field))
                        raise(e)

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
