import os
import argparse
from tqdm import tqdm
import xarray as xr
from subprocess import Popen, PIPE
from concurrent.futures import ProcessPoolExecutor, as_completed


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input', help="path to directory containing data with incorrect time units")
    parser.add_argument(
        'output', help="path to directory where corrected data should be saved")
    parser.add_argument('-t', '--time-offset', type=float, required=True)
    parser.add_argument('-d', '--time-units',
                        default="days since 1850-01-01 00:00:00")
    parser.add_argument('-p', '--processes', default=6, type=int)
    parser.add_argument('-q', '--quiet', action="store_true", help="Suppress progressbars")
    return parser.parse_args()


def fix_units(inpath, outpath, time_units, time_offset):

    with xr.open_dataset(inpath, decode_times=False) as ds:
        if ds['time'].attrs['units'] != time_units:
            ds['time'].attrs['units'] = time_units
            ds = ds.assign_coords(time=ds['time']+time_offset)
            ds = ds.assign_coords(time_bnds=ds['time_bnds']+time_offset)
            ds.to_netcdf(outpath, unlimited_dims=['time'])


def main():
    
    parsed_args = parse_args()

    os.makedirs(parsed_args.output, exist_ok=True)

    with ProcessPoolExecutor(max_workers=parsed_args.processes) as pool:

        files = os.listdir(parsed_args.input)
        futures = []

        for f in files:
            inpath = os.path.join(parsed_args.input, f)
            outpath = os.path.join(parsed_args.output, f)
            futures.append(
                pool.submit(
                    fix_units,
                    inpath,
                    outpath,
                    parsed_args.time_units,
                    parsed_args.time_offset))

        for _ in tqdm(as_completed(futures), total=len(files), disable=parsed_args.quiet):
            pass

    return 0


if __name__ == "__main__":
    exit(main())