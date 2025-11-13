"""Command line interface"""
import sys
import pandas as pd
import json
import argparse

from .odb import get_odb_data_from_file, ODBObsmonData, ODBObsmonVariable
from .obsmon import write_obsmon_sqlite_file, ObsmonVariable


'''
odb2sqlite \
 --run-settings /nobackup/forsk/sm_tryas/git/obsmon-tools/obsmontools/data/run_settings.json \
 --obsmon-config /nobackup/forsk/sm_tryas/git/obsmon-tools/obsmontools/data/obsmon_config.json \
 --odb-config /nobackup/forsk/sm_tryas/git/obsmon-tools/obsmontools/data/odb_config.json \
 --datapath /nobackup/forsk/sm_tryas/harmonie/odb2_in_aa/archive/2025/11/05/12 \
 --suffix mfb \
 --dtg 2025110912 \
 --output ccma.db
'''

def cmd_args_odb2sqlite(argv):

    parser = argparse.ArgumentParser("odb2sqlite")
    parser.add_argument("--run-settings", dest="run_settings", type=str)
    parser.add_argument("--obsmon-config", dest="obsmon_config", type=str)
    parser.add_argument("--odb-config", dest="odb_config", type=str)
    parser.add_argument("--datapath", dest="datapath", type=str)
    parser.add_argument("--suffix", dest="suffix", type=str)
    parser.add_argument("--dtg", dest="dtg", type=str)
    parser.add_argument("--output", dest="output", type=str)

    if len(argv) == 0:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def odb2sqlite(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    kwargs = cmd_args_odb2sqlite(argv)

    run_settings_file = kwargs["run_settings"]
    config_file = kwargs["obsmon_config"]
    odb_config_file = kwargs["odb_config"]
    datapath = kwargs["datapath"]
    suffix = kwargs["suffix"]
    dtg = kwargs["dtg"]
    output_file = kwargs["output"]
    

    obsmon_data = None
    obsmon_vars = []
    with open(run_settings_file, mode="r", encoding="utf8") as fhandler:
        data = json.load(fhandler)
    with open(config_file, mode="r", encoding="utf8") as fhandler:
        config = json.load(fhandler)
    with open(odb_config_file, mode="r", encoding="utf8") as fhandler:
        odb_config = json.load(fhandler)

    obsmon_data = None
    for base in data:
        odb_file = f"{datapath}/{base}.{suffix}"
        print(f"Opening {odb_file}")
        odb_data = get_odb_data_from_file(odb_file)

        for var in data[base]:
            varname = config[var]["varname"]
            obnumber = config[var]["obnumber"]
            obname = config[var]["obname"]
            try:
                satelites = config[var]["satelites"]
            except:
                satelites = ["undefined"]
            levels = [0]
            try:
                levels = config[var]["channels"]
            except:
                pass
            try:
                levels = config[var]["levels"]
            except:
                pass
            print(base, varname, obnumber, obname, satelites, levels)
            for satelite in satelites:
                for level in levels:
                    obvar = ODBObsmonVariable(var, varname, obnumber, obname, base, satname=satelite, level=level)#, instrument=instrument)
                    obsmon_data2 = ODBObsmonData(odb_config, obvar).get_view(odb_data)
                    if obsmon_data is None:
                        obsmon_data = obsmon_data2
                    else:
                        obsmon_data = pd.concat([obsmon_data, obsmon_data2])
                    obsmon_vars.append(obvar)

    write_obsmon_sqlite_file(obsmon_data, obsmon_vars, dtg, output_file)


def cmd_args_json2sqlite(argv):

    parser = argparse.ArgumentParser("json2sqlite")
    parser.add_argument("--qc-file", dest="qc_file", type=str)
    parser.add_argument("--varname", dest="varname", type=str)
    parser.add_argument("--dtg", dest="dtg", type=str)
    parser.add_argument("--output", dest="output", type=str)

    if len(argv) == 0:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def json2sqlite(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    
    kwargs = cmd_args_json2sqlite(argv)

    qc_file = kwargs["qc_file"]
    dtg = kwargs["dtg"]
    varname = kwargs["varname"]
    output_file = kwargs["output"]

    obsmon_vars = [ObsmonVariable("synop_t2m_json", varname, 1, "synop")]
    with open(qc_file, mode="r", encoding="utf8") as fhandler:
        obsmon_data = json.load(fhandler)

    # TODO Add needed columns
    write_obsmon_sqlite_file(obsmon_data, obsmon_vars, dtg, output_file)
