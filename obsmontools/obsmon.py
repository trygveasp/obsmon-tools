"""Obsmon handling."""
import logging

import numpy as np

try:
    import sqlite3
except ImportWarning:
    sqlite3 = None
    logging.warning("Could not import sqlite3 modules")


class ObsmonVariable():

    def __init__(self, tag, varname, obnumber, obname, satname="undefined", level=None):#, instrument=None):
        self.tag = tag
        self.varname = varname
        self.obnumber = obnumber
        self.obname = obname
        self.satname = satname
        self.level = level
        self.surface = False
        self.passive = False


def open_db(dbname):
    """Open database.

    Args:
        dbname (str): File name.

    Raises:
        RuntimeError: Need SQLite

    Returns:
        sqlite3.connect: A connection

    """
    if sqlite3 is None:
        raise RuntimeError("You need SQLITE for obsmon")

    conn = sqlite3.connect(dbname)
    return conn


def close_db(conn):
    """Close data base connection.

    Args:
        conn (sqlite3.connect): Data base connection.
    """
    conn.close()


def create_db(conn, modes, stat_cols):
    """Create data base.

    Args:
        conn (sqlite3.connect): Data base connection.
        modes (_type_): _description_
        stat_cols (_type_): _description_

    """
    cursor = conn.cursor()

    # Create usage table
    cmd = (
        "CREATE TABLE IF NOT EXISTS usage (DTG INT, obnumber INT, obname CHAR(20), "
        "satname CHAR(20), varname CHAR(20), level INT, latitude FLOAT, longitude FLOAT, "
        "statid CHAR(20), obsvalue FLOAT, fg_dep FLOAT, an_dep FLOAT, biascrl FLOAT, "
        "active INT, rejected INT, passive INT, blacklisted INT, anflag INT)"
    )

    cursor.execute(cmd)

    # Create obsmon table
    cmd = (
        "CREATE TABLE IF NOT EXISTS obsmon (DTG INT, obnumber INT, obname CHAR(20), "
        "satname CHAR(20), varname CHAR(20), level INT, passive INT"
    )
    for mode in modes:
        for col in stat_cols:
            cmd = cmd + "," + col + "_" + mode + " FLOAT"

    cmd = cmd + ")"

    cursor.execute(cmd)
    cursor.execute(
        """CREATE INDEX IF NOT EXISTS obsmon_index on usage(DTG,obnumber,obname)"""
    )

    # Save (commit) the changes
    conn.commit()


def populate_usage_db(conn, dtg, observations):
    """Populate usage.

    Args:
        conn (_type_): _description_
        dtg (_type_): _description_
        varname (_type_): _description_
        observations (_type_): _description_

    """
    logging.info("Update usage")

    cursor = conn.cursor()
    # Insert a row of data
    for index, row in observations.iterrows():
        varname = row["varname"]
        obname = row["obname"]
        obnumber = str(row["obnumber"])
        satname = row["satname"]
        level = str(row["level"])
        lon = f"{float(row['lon']):10.5f}"
        lat = f"{float(row['lat']):10.5f}"
        stid = str(row['stid'])
        value = str(row['value'])
        biascrl = str(row['biascrl'])
        anflag = str(row['anflag'])
        if value == "nan":
            value = "NULL"
        if value == "NULL":
            fg_dep = "NULL"
            an_dep = "NULL"
        else:
            fg_dep = "NULL" if np.isnan(row['fg_dep']) else str(row['fg_dep'])
            an_dep = "NULL" if np.isnan(row['an_dep']) else str(row['an_dep'])

        status = int(row['flag'])
        if status == 1: # Active
            istatus = "1,0,0,0"
        elif status == 3: # Active + passive
            istatus = "1,0,1,0"
        elif status == 4: # Rejected
            istatus = "0,0,0,1"
        elif status == 5: # Passive
            istatus = "0,0,1,0"
        elif status == 6: # Passive and rejected
            istatus = "0,0,1,1"
        elif status == 12: # Blacklisted and rejected
            istatus = "0,1,0,1"
        elif status == 14: # Blacklisted, passive and rejected
            istatus = "0,1,1,1"
        else:
            print(status)
            raise NotImplementedError(status)
        
        cmd = (
            "INSERT INTO usage VALUES("
            + str(dtg)
            + ","
            + obnumber
            + ',"'
            + obname
            + '","'
            + satname
            + '","'
            + varname
            + '",'
            + level
            + ","
            + lat
            + ","
            + lon
            + ',"'
            + stid
            + '",'
            + value
            + ","
            + fg_dep
            + ","
            + an_dep
            + ","
            + biascrl
            + ","
            + istatus
            + ","
            + anflag
            + ")"
        )
        #print(cmd)
        logging.info(cmd)
        cursor.execute(cmd)

    # Save (commit) the changes
    conn.commit()
    logging.info("Updated usage")


def rmse(predictions, targets):
    """Root mean square error.

    Args:
        predictions (_type_): _description_
        targets (_type_): _description_

    Returns:
        _type_: _description_

    """
    if len(predictions) > 0:
        return np.sqrt(np.nanmean(((predictions - targets) ** 2)))
    return "NULL"


def absbias(predictions):
    """Absolute bias.

    Args:
        predictions (_type_): _description_

    Returns:
        _type_: _description_

    """
    if len(predictions) > 0:
        return np.nanmean(abs(predictions))
    return "NULL"


def mean(predictions):
    """Mean.

    Args:
        predictions (_type_): _description_

    Returns:
        _type_: _description_

    """
    if len(predictions) > 0:
        return np.nanmean(predictions)
    return "NULL"


def calculate_statistics(observations, modes, stat_cols):
    """Statistics.

    Args:
        observations (_type_): _description_
        modes (_type_): _description_
        stat_cols (_type_): _description_

    Raises:
        NotImplementedError: _description_

    Returns:
        _type_: _description_

    """

    statistics = {}
    for mode in modes:

        if mode == "total":
            subset = observations
        if mode == "land":
            subset = observations[observations["laf"] == float(1)]
        elif mode == "sea":
            subset = observations[observations["laf"] == float(0)]

        obs = subset["value"].to_numpy()
        fg_dep = subset["fg_dep"].to_numpy()
        an_dep = subset["an_dep"].to_numpy()

        for col in stat_cols:
            tab = col + "_" + mode
            if len(obs) > 0:
                fg_bias = mean(fg_dep)
                fg_abs_bias = absbias(fg_dep)
                fg_rms = rmse(np.add(fg_dep, obs), obs)
                fg_dep_tab = mean(fg_dep) # TODO
                fg_uncorr = mean(fg_dep) # TODO
                bc = 0 # TODO
                an_bias = mean(an_dep)
                an_abs_bias = absbias(an_dep)
                an_rms = rmse(np.add(an_dep, obs), obs)
                an_dep_tab = mean(an_dep)
            else:
                fg_bias = 0
                fg_abs_bias = 0
                fg_rms = 0
                fg_dep_tab = 0
                fg_uncorr = 0
                bc = 0
                an_bias = 0
                an_abs_bias = 0
                an_rms = 0
                an_dep_tab = 0

            if col == "nobs":
                statistics.update({tab: len(obs)})
            elif col == "fg_bias":
                statistics.update({tab: fg_bias})
            elif col == "fg_abs_bias":
                statistics.update({tab: fg_abs_bias})
            elif col == "fg_rms":
                statistics.update({tab: fg_rms})
            elif col in "fg_dep":
                statistics.update({tab: fg_dep_tab})
            elif col in "fg_uncorr":
                statistics.update({tab: fg_uncorr})
            elif col == "bc":
                statistics.update({tab: bc})
            elif col == "an_bias":
                statistics.update({tab: an_bias})
            elif col == "an_abs_bias":
                statistics.update({tab: an_abs_bias})
            elif col == "an_rms":
                statistics.update({tab: an_rms})
            elif col == "an_dep":
                statistics.update({tab: an_dep_tab})
            else:
                raise NotImplementedError("Not defined " + col)
    return statistics


def populate_obsmon_db(conn, dtg, data, modes, stat_cols, obsmon_variables):
    """Populate obsmon.

    Args:
        conn (_type_): _description_
        dtg (_type_): _description_
        statistics (_type_): _description_
        modes (_type_): _description_
        stat_cols (_type_): _description_
        obsmon_variable (_type_): _description_

    Raises:
        RuntimeError: _description_

    """
    logging.info("Update obsmon table")

    for obsmon_variable in obsmon_variables:
        varname = obsmon_variable.varname
        obnumber = obsmon_variable.obnumber
        obname = obsmon_variable.obname
        satname = obsmon_variable.satname
        level = obsmon_variable.level
        passive = 0
        if obsmon_variable.passive:
            passive = 1
        #print(varname, obnumber, obname, satname, level)
        #print(len(data), data[["obnumber", "obname", "varname", "satname", "level"]])

        obdata = data[
            (data["obnumber"] == obnumber) &
            (data["obname"] == obname) &
            (data["varname"] == varname) &
            (data["satname"] == satname) &
            (data["level"] == level)
        ]
        #print(len(obdata), obdata[["obnumber", "obname", "varname"]])
        statistics = calculate_statistics(obdata, modes, stat_cols)
        cursor = conn.cursor()
        cmd = (
            "SELECT * FROM obsmon WHERE DTG=="  # noqa
            + dtg
            + " AND obnumber=="
            +str(obnumber)
            + ' AND obname =="'
            + obname
            + '" AND varname=="'
            + varname
            + '" AND LEVEL == '
            + str(level)
            + ' AND satname == "'
            + satname + '"'
        )
        # print(cmd)
        cursor.execute(cmd)
        records = len(cursor.fetchall())
        if records > 1:
            logging.info(cmd)
            raise RuntimeError("You should not have ", records, " in your database")


        if records == 0:
            cmd = (
                "INSERT INTO obsmon VALUES("
                + dtg
                + ","
                + str(obnumber)
                + ',"'
                + obname
                + '","'
                + satname
                + '","'
                + varname
                + '",'
                + str(level)
                + ","
                + str(passive)
            )
        else:
            cmd = "UPDATE obsmon SET "
        first = True
        for mode in modes:
            for col in stat_cols:
                tab = col + "_" + mode
                if records == 0:
                    cmd = cmd + "," + str(statistics[tab]) + ""
                elif first:
                    cmd = cmd + "" + tab + "=" + str(statistics[tab])
                else:
                    cmd = cmd + "," + tab + "=" + str(statistics[tab])
                first = False
        if records == 0:
            cmd = cmd + ")"
        else:
            cmd = (
                cmd
                + " WHERE DTG=="
                + dtg
                + " AND obnumber=="
                + str(obnumber)
                + ' AND obname=="'
                + obname
                + '" AND varname=="'
                + varname
                + '" AND LEVEL == '
                + str(level)
                + ' AND satname == "'
                + satname + '"'
            )

        logging.info(cmd)
        cursor.execute(cmd)
        # Save (commit) the changes
        conn.commit()


def write_obsmon_sqlite_file(obsmon_data, obsmon_variables, dtg, dbname):
    """Write obsmon sqlite file."""

    modes = ["total", "land", "sea"]
    stat_cols = [
        "nobs",
        "fg_bias",
        "fg_abs_bias",
        "fg_rms",
        "fg_dep",
        "fg_uncorr",
        "bc",
        "an_bias",
        "an_abs_bias",
        "an_rms",
        "an_dep",
    ]
    conn = open_db(dbname)
    create_db(conn, modes, stat_cols)

    populate_usage_db(conn, dtg, obsmon_data)
    populate_obsmon_db(
        conn,
        dtg,
        obsmon_data,
        modes,
        stat_cols,
        obsmon_variables
    )
    close_db(conn)
