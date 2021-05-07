import os
import re
import json
from enum import Enum

from pathlib import Path
from pprint import pprint
from datetime import datetime
from warehouse.util import load_file_lines, sproket_with_id, get_last_status_line


class DatasetStatus(Enum):
    UNITITIALIZED = 1
    INITIALIZED = 2
    RUNNING = 4
    FAILED = 5
    SUCCESS = 6
    IN_WAREHOUSE = 8
    IN_PUBLICATION = 9
    NOT_PUBLISHED = 10
    NOT_IN_PUBLICATION = 11
    NOT_IN_WAREHOUSE = 12
    PARTIAL_PUBLISHED = 13


class DatasetStatusMessage(Enum):
    PUBLICATION_READY = "PUBLICATION:Ready:"
    WAREHOUSE_READY = "WAREHOUSE:Ready:"
    VALIDATION_READY = "VALIDATION:Ready:"


non_binding_status = ['Blocked:', 'Unblocked:', 'Approved:', 'Unapproved:']


SEASONS = [{
    'name': 'ANN',
    'start': '01',
    'end': '12'
}, {
    'name': 'DJF',
    'start': '01',
    'end': '12'
}, {
    'name': 'MAM',
    'start': '03',
    'end': '05'
}, {
    'name': 'JJA',
    'start': '06',
    'end': '08'
}, {
    'name': 'SON',
    'start': '09',
    'end': '11'
}]


class Dataset(object):
    def get_status_from_archive(self):
        ...

    def initialize_status_file(self):

        if not self.status_path.exists():
            self.status_path.touch(mode=0o755, exist_ok=True)

        found_id = False
        with open(self.status_path, 'r') as instream:
            for line in instream.readlines():
                if 'DATASETID' in line:
                    found_id = True
                    break
        if not found_id:
            with open(self.status_path, 'a') as outstream:
                outstream.write(f'DATASETID={self.dataset_id}\n')

        if (status := get_last_status_line(self.status_path)):
            status_attrs = status.split(':')
            self._status = f"{status_attrs[-3]}:{status_attrs[-2]}:"
        else:
            self._status = DatasetStatus.UNITITIALIZED.name

    def __init__(self, dataset_id, status_path, pub_base=None, warehouse_base=None, archive_base=None, start_year=None, end_year=None, datavars=None, path='', versions={}, stat=None, comm=None, *args, **kwargs):
        super().__init__()
        self.dataset_id = dataset_id
        self._status = DatasetStatus.UNITITIALIZED.name

        self.data_path = None
        self.start_year = start_year
        self.end_year = end_year
        self.datavars = datavars
        self.missing = None
        self._publication_path = Path(path) if path != '' else None
        self.pub_base = pub_base
        self.warehouse_path = Path(path) if path != '' else None
        self.warehouse_base = warehouse_base
        self.archive_path = Path(path) if path != '' else None
        self.status_path = status_path

        self.archive_base = archive_base
        self.sproket = kwargs.get('sproket')

        self.stat = stat if stat else {}
        self.comm = comm if comm else []

        self.versions = versions

        facets = self.dataset_id.split('.')
        if facets[0] == 'CMIP6':
            self.project = 'CMIP6'
            self.data_type = 'CMIP'
            self.activity = facets[1]
            self.model_version = facets[3]
            self.experiment = facets[4]
            self.ensemble = facets[5]
            self.table = facets[6]
            self.resolution = None
            if facets[6] in ['Amon', '3hr', 'day']:
                self.realm = 'atmos'
            elif facets[6] == 'Lmon':
                self.realm = 'land'
            elif facets[6] == 'Omon':
                self.realm = 'ocean'
            elif facets[6] == 'SImon':
                self.realm = 'sea-ice'
            else:
                self.realm = 'fixed'
            self.freq = None  # the frequency and realm are part of the CMIP table
            self.grid = 'gr'
            self.warehouse_path = Path(
                self.warehouse_base,
                self.project,
                self.activity,
                'E3SM-Project',
                self.model_version,
                self.experiment,
                self.ensemble,
                self.table,
                self.grid)
        else:
            self.project = 'E3SM'
            self.model_version = facets[1]
            self.experiment = facets[2]
            self.resolution = facets[3]
            self.realm = facets[4]
            self.grid = facets[5]
            self.data_type = facets[6]
            self.freq = facets[7]
            self.ensemble = facets[8]
            self.activity = None
            self.table = None
            self.warehouse_path = Path(
                self.warehouse_base,
                self.project,
                self.model_version,
                self.experiment,
                self.resolution,
                self.realm,
                self.grid,
                self.data_type,
                self.freq,
                self.ensemble)

        self.initialize_status_file()

    def update_from_status_file(self):
        self.load_dataset_status_file()
        latest = get_last_status_line(self.status_path).split(':')

        new_status = ":".join(latest[3:]).strip()
        self._status = new_status

    @property
    def latest_warehouse_dir(self):
        if self.warehouse_path is None or (not self.warehouse_path and not self.warehouse_path.exists()):
            raise ValueError(
                f"The dataset {self.dataset_id} does not have a warehouse path")
        if not self.warehouse_path.exists():
            self.warehouse_path.mkdir(parents=True, exist_ok=True)

        # we assume that the warehouse directory contains only directories named "v0.#" or "v#"
        try:
            latest_version = sorted([float(str(x.name)[1:]) for x in self.warehouse_path.iterdir(
            ) if x.is_dir() and any(x.iterdir()) and 'tmp' not in x.name]).pop()
        except IndexError:
            latest_version = 0

        if not isinstance(latest_version, int) and latest_version.is_integer():
            latest_version = int(latest_version)

        if latest_version < 0.1:
            latest_version = 0
        
        path_to_latest = Path(self.warehouse_path, f"v{latest_version}").resolve()
        if not path_to_latest.exists():
            path_to_latest.mkdir(parents=True)
        return str(path_to_latest)

    @property
    def latest_pub_dir(self):
        # we assume that the publication directory contains only directories named "v0.#" or "v#"
        try:
            latest_version = sorted([float(
                str(x.name)[1:]) for x in self.publication_path.iterdir() if x.is_dir()]).pop()
        except IndexError:
            latest_version = "0"
        if latest_version.is_integer():
            latest_version = int(latest_version)
        return str(Path(self.publication_path, f"v{latest_version}").resolve())

    @property
    def pub_version(self):
        """
        Returns the latest version number in the publication directory. If not version exists
        then it returns 0
        """
        if not self.publication_path or not self.publication_path.exists():
            return 0

        # we assume that the publication directory contains only directories named "v0.#" or "v#"
        try:
            latest_version = sorted([float(
                str(x.name)[1:]) for x in self.publication_path.iterdir() if x.is_dir()]).pop()
        except IndexError:
            return 0
        return int(latest_version)

    @property
    def publication_path(self):
        if not self._publication_path or not self._publication_path.exists():
            if self.project == 'CMIP6':
                pubpath = Path(self.pub_base, self.project, self.activity,
                               self.model_version, self.experiment, self.ensemble,
                               self.table, self.grid)
            else:
                pubpath = Path(self.pub_base, self.project, self.model_version,
                               self.experiment, self.resolution, self.realm,
                               self.grid, self.data_type, self.freq, self.ensemble)
            self._publication_path = pubpath
            self._publication_path.mkdir(parents=True, exist_ok=True)
        return self._publication_path

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        if status is None or status == self._status:
            return
        params = None
        if isinstance(status, tuple):
            status, params = status

        self._status = status
        with open(self.status_path, 'a') as outstream:
            msg = f'STAT:{datetime.now().strftime("%Y%m%d_%H%M%S")}:WAREHOUSE:{status}'
            if params:
                items = [f"{k}={v}".replace(":", "^")
                         for k, v in params.items()]
                msg += ",".join(items)
            outstream.write(msg + "\n")

    def lock(self, path):
        if self.is_locked(path):
            return
        path = Path(path)
        if not path.exists():
            return
        Path(path, '.lock').touch()

    def is_locked(self, path=None):
        if path is None:
            path = self.latest_warehouse_dir
        else:
            path = Path(path)

        if not path.exists():
            return False
        for item in path.glob('.lock'):
            return True
        return False

    def unlock(self, path):
        Path(path, '.lock').unlink(missing_ok=True)

    def datatype_from_id(self):
        if 'CMIP' in self.dataset_id:
            return 'CMIP'

        facets = self.dataset_id.split('.')
        # should be component.grid.type.freq, e.g. "atmos.180x360.time-series.mon"
        return '.'.join(facets[4:7])

    def get_latest_status(self):
        latest = '0'
        latest_val = None
        second_latest = None
        for major in self.stat.keys():
            for minor in self.stat[major].keys():
                for item in self.stat[major][minor]:
                    if item[0] >= latest \
                            and item[1] not in non_binding_status:
                        latest = item[0]
                        second_latest = latest_val
                        latest_val = f'{major}:{minor}:{item[1]}'
        return latest_val, second_latest

    def check_dataset_is_complete(self, files):

        # filter out files from old versions
        nfiles = []
        for file in files:
            file_path, name = os.path.split(file)
            file_attrs = file_path.split('/')
            version = file_attrs[-1]
            nfiles.append((version, file))

        latest_version = sorted(nfiles)[-1][0]
        files = [x for version, x in nfiles
                 if version == latest_version
                 and x != '.lock' and x != '.mapfile']

        self.versions[latest_version] = len(files)

        if not self.start_year or not self.end_year:
            if 'CMIP' in self.dataset_id:
                self.start_year, self.end_year = self.infer_start_end_cmip(
                    files)
            else:
                if self.data_type == 'time-series':
                    self.start_year, self.end_year = self.get_ts_start_end(
                        files[0])
                elif self.data_type == 'climo':
                    self.start_year, self.end_year = self.infer_start_end_climo(
                        files)
                else:
                    self.start_year, self.end_year = self.infer_start_end_e3sm(
                        files)

        if self.data_type == 'CMIP':
            if 'fx' in self.dataset_id:
                if files:
                    return True
                else:
                    self.missing = [self.dataset_id]
                    return False
            self.missing = self.check_spans(files)
        else:
            if 'model-output.mon' in self.dataset_id:
                self.missing = self.check_monthly(files)
            elif 'climo' in self.dataset_id:
                self.missing = self.check_climos(files)
            elif 'time-series' in self.dataset_id:
                self.missing = self.check_time_series(files)
            elif 'fixed' in self.dataset_id:
                # missing, extra = check_fixed(files, dataset_id, spec)
                # TODO: implement this
                self.missing = []
            else:
                self.missing = self.check_submonthly(files)

        if self.missing:
            return False
        else:
            return True

    def get_esgf_status(self, sproket='sproket'):
        """
        Check ESGF to see of the dataset has already been published,
        if it exists check that the dataset is complete"""

        # TODO: fix this at some point
        if self.table == 'fx':
            return DatasetStatus.SUCCESS

        _, files = sproket_with_id(
            f"{self.dataset_id}*", sproket_path=self.sproket)
        if not files:
            return DatasetStatus.UNITITIALIZED

        latest_version = 0
        for f in files:
            version_dir = int(f.split(os.sep)[-2][1:])
            if version_dir > latest_version:
                latest_version = version_dir

        files = [x for x in files if x.split(
            os.sep)[-2] == f"v{latest_version}"]

        if self.check_dataset_is_complete(files):
            return DatasetStatus.SUCCESS
        else:
            return DatasetStatus.PARTIAL_PUBLISHED

    def get_status_from_pub_dir(self):

        if not self.publication_path.exists():
            return DatasetStatus.NOT_IN_PUBLICATION

        self.update_versions(self.publication_path)

        version_names = list(self.versions.keys())
        version_dir = Path(self.publication_path, version_names[-1])
        files = [str(x.resolve()) for x in version_dir.glob('*')]
        if not files:
            return DatasetStatus.NOT_IN_PUBLICATION

        is_complete = self.check_dataset_is_complete(files)
        if is_complete:
            # we only set the status file if the publication is complete
            # otherwise the "official" location should be the warehouse
            self.status = DatasetStatusMessage.PUBLICATION_READY.value

            return DatasetStatus.IN_PUBLICATION
        else:
            return DatasetStatus.NOT_IN_PUBLICATION

    def update_versions(self, path):
        self.versions = {
            x: len([i for i in Path(path, x).glob('*')])
            for x in os.listdir(path) if x[0] == 'v'
        }

    def get_status_from_warehouse(self):
        
        if not self.warehouse_path.exists():
            return DatasetStatus.NOT_IN_WAREHOUSE

        self.update_versions(self.warehouse_path)

        version_names = list(self.versions.keys())
        version_dir = Path(self.publication_path, version_names[-1])
        files = [x.resolve() for x in version_dir.glob('*')]
        if not files:
            return DatasetStatus.NOT_IN_WAREHOUSE
        else:
            if self.check_dataset_is_complete(files):
                return DatasetStatus.IN_WAREHOUSE
            else:
                return DatasetStatus.NOT_IN_WAREHOUSE

    def find_status(self, sproket='sproket'):
        """
        Lookup the datasets status in ESGF, or on the filesystem
        """

        # if the dataset is UNITITIALIZED, then we need to build up the status from scratch
        if self.status == DatasetStatus.UNITITIALIZED:
            # returns either NOT_PUBLISHED or SUCCESS or PARTIAL_PUBLISHED or UNITITIALIZED
            self.status = self.get_esgf_status(sproket)

        # TODO: figure out how to update and finish a dataset thats been published
        # but is missing some of its files
        # if self.status == DatasetStatus.PARTIAL_PUBLISHED:
        #    ...

        if self.status in [DatasetStatus.NOT_PUBLISHED, DatasetStatus.UNITITIALIZED]:
            # returns IN_PUBLICATION or NOT_IN_PUBLICATION
            self.status = self.get_status_from_pub_dir()
            ...

        if self.status in [DatasetStatus.NOT_IN_PUBLICATION, DatasetStatus.UNITITIALIZED]:
            # returns IN_WAREHOUSE or NOT_IN_WAREHOUSE
            self.status = self.get_status_from_warehouse()
            ...

        if self.status in [DatasetStatus.NOT_IN_WAREHOUSE, DatasetStatus.UNITITIALIZED] and self.data_type not in ['time-series', 'climo']:
            # returns IN_ARCHIVE OR NOT_IN_ARCHIVE
            # self.status = self.get_status_from_archive()
            ...

        return self.dataset_id, self.status, self.missing

    def check_submonthly(self, files):

        missing = list()
        files = sorted(files)

        first = files[0]
        pattern = re.compile(r'\d{4}-\d{2}.*nc')
        if not (idx := pattern.search(first)):
            raise ValueError(f'Unexpected file format: {first}')

        prefix = first[:idx.start()]
        # TODO: Come up with a way of doing this check more
        # robustly. Its hard because the high-freq files arent consistant
        # from case to case, using different 'h' codes and different frequencies
        # for the time being, if there's at least one file per year it'll get marked as correct
        for year in range(self.start_year, self.end_year):
            found = None
            pattern = re.compile(f'{year:04d}' + r'-\d{2}.*.nc')
            for idx, file in enumerate(files):
                if pattern.search(file):
                    found = idx
                    break
            if found is not None:
                files.pop(idx)
            else:
                name = f'{prefix}{year:04d}'
                missing.append(name)

        return missing

    def check_time_series(self, files):

        missing = []
        files = [x.split('/')[-1] for x in sorted(files)]
        files_found = []

        if not self.datavars:
            raise ValueError(
                f"dataset {self.dataset_id} is trying to validate time-series files, but has no datavars")

        for var in self.datavars:

            # depending on the mapping file used to regrid the time-series
            # they may have different names, so we start by finding
            # all the files for each variable
            v_files = list()
            for x in files:
                idx = -36 if 'cmip6_180x360_aave' in x else -17
                if var in x and x[:idx] == var:
                    v_files.append(x)

            if not v_files:
                missing.append(
                    f'{self.dataset_id}-{var}-{self.start_year:04d}-{self.end_year:04d}')
                continue

            v_files = sorted(v_files)
            v_start, v_end = self.get_ts_start_end(v_files[0])
            if self.start_year != v_start:
                missing.append(
                    f'{self.dataset_id}-{var}-{self.start_year:04d}-{v_start:04d}')

            prev_end = self.start_year
            for file in v_files:
                file_start, file_end = self.get_ts_start_end(file)
                if file_start == self.start_year:
                    prev_end = file_end
                    continue
                if file_start == prev_end + 1:
                    prev_end = file_end
                else:
                    missing.append(
                        f"{self.dataset_id}-{var}-{prev_end:04d}-{file_start:04d}")

            file_start, file_end = self.get_ts_start_end(files[-1])
            if file_end != self.end_year:
                missing.append(
                    f"{self.dataset_id}-{var}-{file_start:04d}-{self.end_year:04d}")

        return missing

    def check_monthly(self, files):
        """
        Given a list of monthly files, find any that are missing
        """
        missing = []
        files = sorted(files)

        pattern = r'\d{4}-\d{2}.*nc'
        try:
            idx = re.search(pattern=pattern, string=files[0])
        except Exception as e:
            raise ValueError(
                f"file {files[0]} does not match expected pattern for monthly files")

        if not idx:
            raise ValueError(f'Unexpected file format: {files[0]}')

        prefix = files[0][:idx.start()]
        suffix = files[0][idx.start() + 7:]

        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                name = f'{prefix}{year:04d}-{month:02d}{suffix}'
                if name not in files:
                    missing.append(name)

        return missing

    def check_climos(self, files):
        """
        Given a list of climo files, find any that are missing
        """
        missing = []

        pattern = r'_\d{6}_\d{6}_climo.nc'
        files = sorted(files)
        idx = re.search(pattern=pattern, string=files[0])
        if not idx:
            raise ValueError(f'Unexpected file format: {files[0]}')
        prefix = files[0][:idx.start() - 2]

        for month in range(1, 13):
            name = f'{prefix}{month:02d}_{self.start_year:04d}{month:02d}_{self.end_year:04d}{month:02d}_climo.nc'
            if name not in files:
                missing.append(name)

        for season in SEASONS:
            name = f'{prefix}{season["name"]}_{self.start_year:04d}{season["start"]}_{self.end_year:04d}{season["end"]}_climo.nc'
            if name not in files:
                missing.append(name)

        return missing

    @staticmethod
    def get_file_start_end(filename):
        if 'clim' in filename:
            return int(filename[-21:-17]), int(filename[-14: -10])
        else:
            return int(filename[-16:-12]), int(filename[-9: -5])

    @staticmethod
    def get_ts_start_end(filename):
        p = re.compile(r'_\d{6}_\d{6}.*nc')
        idx = p.search(filename)
        if not idx:
            raise ValueError(f'Unexpected file format: {filename}')
        start = int(filename[idx.start() + 1: idx.start() + 5])
        end = int(filename[idx.start() + 8: idx.start() + 12])
        return start, end

    def check_spans(self, files):
        """
        Given a list of CMIP files, find of all the files that should be there are
        """
        missing = []
        files = sorted(files)
        file_start, file_end = self.get_file_start_end(files[0])

        if file_start != self.start_year:
            msg = f"{self.dataset_id}-{self.start_year:04d}-{file_start:04d} -> expected case start doesnt match files start"
            missing.append(msg)
            prev_end = file_start - 1
        else:
            prev_end = self.start_year

        for file in files:
            file_start, file_end = self.get_file_start_end(file)
            if file_start == self.start_year:
                prev_end = file_end
                continue
            if file_start != prev_end + 1:
                msg = f"{self.dataset_id}-{prev_end:04d}-{file_start:04d}"
                missing.append(msg)
            prev_end = file_end

        file_start, file_end = self.get_file_start_end(files[-1])
        if file_end != self.end_year:
            msg = f"{self.dataset_id}-{file_end:04d}-{self.end_year:04d} -> expected case end doesnt match files end"
            missing.append(msg)
        return missing

    def infer_start_end_cmip(self, files):
        """
        From a list of files with the given naming convention
        return the start year of the first file and the end year of the
        last file

        A typical CMIP6 file will have a name like:
        pbo_Omon_E3SM-1-1-ECA_hist-bgc_r1i1p1f1_gr_185001-185412.nc' 
        """
        files = sorted(files)
        first, last = files[0], files[-1]
        p = r'\d{6}-\d{6}'
        idx = re.search(pattern=p, string=first)
        if not idx:
            return None, None
        start = int(first[idx.start(): idx.start() + 4])
        idx = re.search(pattern=p, string=last)
        end = int(last[idx.start() + 7: idx.start() + 11])
        return start, end

    def infer_start_end_e3sm(self, files):
        """
        From a list of files with the given naming convention
        return the start year of the first file and the end year of the
        last file
        """
        f = sorted(files)
        p = r'\.\d{4}-\d{2}'
        idx = re.search(pattern=p, string=f[0])
        if not idx:
            return None, None
        start = int(f[0][idx.start() + 1: idx.start() + 5])
        idx = re.search(pattern=p, string=f[-1])
        end = int(f[-1][idx.start() + 1: idx.start() + 5])
        return start, end

    @staticmethod
    def infer_start_end_climo(files):
        f = sorted(files)
        p = r'_\d{6}_\d{6}_'
        idx = re.search(pattern=p, string=f[0])
        start = int(f[0][idx.start() + 1: idx.start() + 5])

        idx = re.search(pattern=p, string=f[-1])
        end = int(f[-1][idx.start() + 8: idx.start() + 12])

        return start, end

    def is_blocked(self, state):
        if not self.status_path or not self.status_path.exists():
            raise ValueError(
                f"Status file for {self.dataset_id} cannot be found")

        # reload the status file in case somethings changed
        self.load_dataset_status_file(self.status_path)

        status_attrs = state.split(':')
        blocked = False
        if status_attrs[0] in self.stat['WAREHOUSE'].keys():
            state_messages = sorted(self.stat['WAREHOUSE'][status_attrs[0]])
            for ts, message in state_messages:
                if 'Blocked' not in message and 'Unblocked' not in message:
                    continue
                message_items = message.split(':')
                if len(message_items) < 2:
                    continue
                if message_items[0] not in state:
                    continue
                if 'Blocked' in message_items[1]:
                    blocked = True
                elif 'Unblocked' in message_items[1]:
                    blocked = False
        return blocked

    def __str__(self):
        return f"""id: {self.dataset_id},
path: {self.data_path},
version: {', '.join(self.versions.keys())},
stat: {self.get_latest_status()},
comm: {self.comm}"""

    def load_dataset_status_file(self, path=None):
        """
        read status file, convert lines "STAT:ts:PROCESS:status1:status2:..."
        into dictionary, key = STAT, rows are tuples (ts,'PROCESS:status1:status2:...')
        and for comments, key = COMM, rows are comment lines
        """
        if path is None:
            path = self.status_path

        if not path.exists():
            return dict()
        self.status_path = path

        statbody = load_file_lines(path.resolve())
        for line in statbody:
            line_info = line.split(':')
            # forge tuple (timestamp,residual_string), add to STAT list
            if line_info[0] == 'STAT':
                timestamp = line_info[1]
                major = line_info[2]
                minor = line_info[3]
                status = line_info[4]
                if len(line_info) > 5:
                    args = line_info[5:]

                if major not in self.stat:
                    self.stat[major] = {}
                if minor not in self.stat[major]:
                    self.stat[major][minor] = []

                message = (timestamp, ':'.join(line_info[4:]))
                # make sure not to load duplicate messages
                if message not in self.stat[major][minor]:
                    self.stat[major][minor].append(message)
            else:
                self.comm.append(line)
        return
