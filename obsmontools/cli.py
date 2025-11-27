"""Command line interface"""
import os
import sys
import json
import argparse
from contextlib import suppress
import pandas as pd

from .odb import get_odb_data_from_file, ODBObsmonData, ODBObsmonVariable
from .obsmon import write_obsmon_sqlite_file, ObsmonVariable


def cmd_args_odb2sqlite(argv):
    """Get arguments for command

    Args:
        argv (list): Input arguments

    Returns:
       dict: Parser settings
    """

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
    """Get arguments for command

            Args:
        argv (list): Input arguments
    """

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

        if os.path.exists(odb_file) and os.path.getsize(odb_file) > 0:
            odb_data = get_odb_data_from_file(odb_file)
        else:
            print(f"File {odb_file} is missing or empty")
            break

        for var in data[base]:
            varname = config[var]["varname"]
            obnumber = config[var]["obnumber"]
            obname = config[var]["obname"]
            try:
                satelites = config[var]["satelites"]
            except KeyError:
                satelites = ["undefined"]
            levels = [0]
            with suppress(KeyError):
                levels = config[var]["channels"]
            with suppress(KeyError):
                levels = config[var]["levels"]

            print(base, varname, obnumber, obname, satelites, levels)
            for satelite in satelites:
                for level in levels:
                    obvar = ODBObsmonVariable(
                        var, varname, obnumber, obname, base,
                        satname=satelite, level=level
                    )
                    obsmon_data2 = ODBObsmonData(odb_config, obvar).get_view(odb_data)
                    if obsmon_data is None:
                        obsmon_data = obsmon_data2
                    else:
                        obsmon_data = pd.concat([obsmon_data, obsmon_data2])
                    obsmon_vars.append(obvar)

    if obsmon_data is not None:
        write_obsmon_sqlite_file(obsmon_data, obsmon_vars, dtg, output_file)


def cmd_args_json2sqlite(argv):
    """Get arguments for command

    Args:
        argv (list): Input arguments
    """

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
    """Get arguments for command

    Args:
        argv (list, optional): Input arguments. Default to None
    """

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
