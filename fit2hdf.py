#!/usr/bin/env python2

import os
import logging
import fitparse
import h5py

logging.basicConfig(level=logging.INFO)

ffn = '/tmp/gar/activity_2144872935.fit'
#ffn = '/tmp/gar/activity_2145792613.fit'
hfn = '{0}.{1}'.format(os.path.splitext(ffn)[0],'hdf5')


with fitparse.FitFile(ffn) as f:
    records = list(f.get_messages('record'))
N = len(records)

logging.info('opening hdf5 file: {}'.format(hfn))
#TODO# with ... as ...
hf = h5py.File(hfn, 'w')
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
                        compression='gzip')
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
                            compression='gzip')
                    hf[field.name].attrs.create('unit', 'seconds since 1989-12-31 00:00:00 UTC')
                    hf[field.name][i] = field.raw_value
                    #TODO# consider revising to unix epoch
                    logging.info('added {0.name} to dataset using type of uint32 and units of {1}'.format(field,hf[field.name].attrs.get('unit')))
                elif field.type.name.startswith("sint"):
                    hf.create_dataset(field.name, (N,),
                            dtype=field.type.name[1:],
                            compression='gzip')
                    hf[field.name].attrs.create('unit', field.units)
                    hf[field.name][i] = field.value
                    logging.info('added {0.name} to dataset using type of int8 and units of {0.units}'.format(field))
                elif field.type.name == "activity_type":
                    hf.attrs.create('activity_type', field.value)
                else:
                    logging.warning('not sure what do with type: {0.type.name} for field {0.name}'.format(field))
                    raise(e)

hf.close()
#TODO# set creation time to match original file, modification time to current
