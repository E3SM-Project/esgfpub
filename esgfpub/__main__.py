"""
A tool for automating much of the ESGF publication process
"""

from esgfpub.util import transfer_files, mapfile_gen, validate_raw, makedir, parse_args
from esgfpub.util import print_message
from esgfpub.publication_checker import publication_checker
import sys
import os
from threading import Event
from configobj import ConfigObj
from tqdm import tqdm


def check(ARGS):
    return publication_checker(
        spec_path=ARGS.case_spec,
        data_path=ARGS.data_path,
        cases=ARGS.cases,
        ens=ARGS.ens,
        tables=ARGS.tables,
        variables=ARGS.variables,
        published=ARGS.published,
        sproket=ARGS.sproket,
        max_connections=ARGS.max_connections,
        serial=ARGS.serial,
        debug=ARGS.debug,
        dataset_ids=ARGS.dataset_ids,
        projects=ARGS.project,
        to_json=ARGS.to_json)


def publish(ARGS):
    if not ARGS.config:
        PARSER.print_help()
        return 1

    if ARGS.over_write:
        overwrite = True
    else:
        overwrite = False

    try:
        CONFIG = ConfigObj(ARGS.config)
    except SyntaxError as error:
        print_message("Unable to parse config file")
        print(repr(error))
        return 1

    try:
        BASEOUTPUT = CONFIG['output_path']
        ATMRES = CONFIG['atmospheric_resolution']
        OCNRES = CONFIG['ocean_resolution']
        DATA_PATHS = CONFIG['data_paths']
        ENSEMBLE = CONFIG['ensemble']
        EXPERIMENT_NAME = CONFIG['experiment']
        GRID = CONFIG.get('non_native_grid')
        START = int(CONFIG['start_year'])
        END = int(CONFIG['end_year'])
    except ValueError as error:
        print_message('Unable to find values in config file')
        print(repr(error))
        return 1

    print_message('Validating raw data', 'ok')
    if not validate_raw(DATA_PATHS, START, END):
        return 1

    resdirname = "{}_atm_{}_ocean".format(ATMRES, OCNRES)
    makedir(os.path.join(BASEOUTPUT, EXPERIMENT_NAME, resdirname))

    transfer_mode = ARGS.transfer_mode
    if transfer_mode == 'move':
        print_message('Moving files', 'ok')
    elif transfer_mode == 'copy':
        print_message('Copying files', 'ok')
    elif transfer_mode == 'link':
        print_message('Linking files', 'ok')
    num_moved = transfer_files(
        outpath=BASEOUTPUT,
        experiment=EXPERIMENT_NAME,
        grid=GRID,
        mode=transfer_mode,
        data_paths=DATA_PATHS,
        ensemble=ENSEMBLE,
        overwrite=overwrite)
    if num_moved == -1:
        return 1

    RUNMAPS = CONFIG.get('mapfiles', False)
    if not RUNMAPS or RUNMAPS not in [True, 'true', 'True', 1, '1']:
        print_message('Not running mapfile generation', 'ok')
        print_message('Publication prep complete', 'ok')
        return 0
    else:
        print_message('Starting mapfile generation', 'ok')

    try:
        INIPATH = CONFIG['ini_path']
        MAPOUT = ARGS.mapout
    except:
        raise ValueError(
            "Mapfiles generation is turned on, but the config is missing the ini_path option")
    NUMWORKERS = CONFIG.get('num_workers', 4)
    event = Event()

    pbar = tqdm(
        desc="Generating mapfiles",
        total=num_moved)
    try:
        res = mapfile_gen(
            basepath=BASEOUTPUT,
            inipath=INIPATH,
            experiment=EXPERIMENT_NAME,
            outpath=MAPOUT,
            maxprocesses=NUMWORKERS,
            event=event,
            pbar=pbar)
    except KeyboardInterrupt as error:
        print_message('Keyboard interrupt ... exiting')
        event.set()
        return 1
    else:
        if res == 0:
            print_message('Publication prep complete', 'ok')
        else:
            print_message(
                'mapfile generation exited with status: {}'.format(res), 'error')
        return res


def main():

    ARGS = parse_args()
    subcommand = ARGS.subparser_name
    if subcommand == 'check':
        return check(ARGS)
    elif subcommand == 'publish':
        return publish(ARGS)
    else:
        raise ValueError("Unrecognised subcommand")


if __name__ == "__main__":
    sys.exit(main())
