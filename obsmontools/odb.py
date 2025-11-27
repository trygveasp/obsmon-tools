"""ODB handling."""
import pyodc as odc

from .obsmon import ObsmonVariable


class ODBObsmonVariable(ObsmonVariable):

    def __init__(self, tag, varname, obnumber, obname, view, satname="undefined", level=None):
        ObsmonVariable.__init__(self, tag, varname, obnumber, obname, satname=satname, level=level)
        self.view = view
        self.instrument = obname


class ODBObsmonData():

    def __init__(self, odb_config, obsmon_variable):
        self.config = odb_config
        self.obsmon_variable = obsmon_variable
        self.tag = self.obsmon_variable.tag
        self.view = self.obsmon_variable.view
        self.varno = self.config[self.tag]["varnr"]
        try:
            codetypes = self.config[self.tag]["codetypes"]
        except KeyError:
            codetypes = None
        self.codetypes = codetypes
        self.obstype = self.obsmon_variable.obnumber
        self.level = self.obsmon_variable.level
        self.satname = self.obsmon_variable.satname
        self.instrument = self.obsmon_variable.instrument
        self.channel = self.obsmon_variable.level
        self.instrument_id = self.get_instrument_id()
        self.satelite_id = self.get_satelite_id()
        self.missing = -1e36
        self.passive = False

    def get_view(self, df_decoded):
        if self.view == "conv":
            observations = self.filter_odb_conv_data(df_decoded)
        elif self.view == "radar":
            observations = self.filter_odb_radar_data(df_decoded)
        elif self.view == "mwrad":
            observations = self.filter_odb_mwrad_data(df_decoded)
        elif self.view == "amv":
            observations = self.filter_odb_amv_data(df_decoded)
        elif self.view == "irrad":
            observations = self.filter_odb_irrad_data(df_decoded)
        elif self.view == "scatt":
            observations = self.filter_odb_scatt_data(df_decoded)
        else:
            raise NotImplementedError(self.view)

        observations = observations.rename(
            columns={
                'fg_depar@body': 'fg_dep',
                'an_depar@body': 'an_dep',
                'lon@hdr': 'lon',
                'lat@hdr': 'lat',
                'statid@hdr': 'stid',
                'obsvalue@body': 'value',
                'datum_status@body': 'flag',
                'lsm@modsurf': 'laf',
                'datum_anflag@body': 'anflag',
            })

        observations = observations[[
            "lon", "lat", "stid", "value", "fg_dep", "an_dep", "flag", "laf", "biascrl", "anflag" 
        ]]

        osize = len(observations)
        extra = {
            "varname": [self.obsmon_variable.varname for i in range(0,osize)],
            "obname": [self.obsmon_variable.obname for i in range(0,osize)],
            "obnumber": [self.obsmon_variable.obnumber for i in range(0,osize)],
            "satname": [self.obsmon_variable.satname for i in range(0,osize)],
            "level": [self.obsmon_variable.level for i in range(0,osize)],
        }
        observations = observations.assign(**extra)
        # Mark observation as passive
        if any(item in observations[["flag"]] for item in [3,5,7]):
            self.passive = True

        return observations

    def get_satelite_id(self):
        if self.satname == "undefined":
            return None
        return self.config["satelites"][self.satname]["id"]

    def get_instrument_id(self):
        if self.satname == "undefined" or self.instrument is None:
            return None
        if self.instrument not in self.config["satelites"][self.satname]["instruments"]:
            raise RuntimeError("Instrument is not on board this satelite?")
        return self.config["instrument_ids"][self.instrument]

    def filter_odb_conv_data(self, df_decoded):

        observations = df_decoded[
            (df_decoded["obstype@hdr"] == self.obstype) &
            (df_decoded["varno@body"] == self.varno)
        ]
        if self.codetypes is not None:
            observations = observations[
                (observations["codetype@hdr"].isin(self.codetypes))
            ]
        osize = len(observations)
        extra = {
            "biascrl": [0.0 for i in range(0,osize)],
        }
        observations = observations.assign(**extra)     
        return observations

    def filter_odb_amv_data(self, df_decoded):

        observations = df_decoded[
            (df_decoded["obstype@hdr"] == self.obstype) &
            (df_decoded["varno@body"] == self.varno)
        ]
        if self.codetypes is not None:
            observations = observations[
                (observations["codetype@hdr"].isin(self.codetypes))
            ]
        osize = len(observations)
        extra = {
            "biascrl": [0.0 for i in range(0,osize)],
            "laf": [0 for i in range(0,osize)],
            "stid": ["NA" for i in range(0,osize)],
        }
        observations = observations.assign(**extra)     
        return observations

    def filter_odb_mwrad_data(self, df_decoded):

        if self.satelite_id is None or self.instrument_id is None or self.channel is None:
            raise RuntimeError("Needed sensor information is missing")

        # odc header
        observations = df_decoded[
            (df_decoded["obstype@hdr"] == self.obstype) &
            (df_decoded["varno@body"] == self.varno) &
            (df_decoded["sensor@hdr"] == self.instrument_id) &
            (df_decoded["satellite_identifier@sat"] == self.satelite_id) &
            (df_decoded["vertco_reference_1@body"] == self.channel) &
            (df_decoded["an_depar@body"] > self.missing)
        ]
        observations = observations.rename(columns={
            'biascorr@body': 'biascrl',
        })
        return observations

    def filter_odb_irrad_data(self, df_decoded):

        if self.satelite_id is None or self.instrument_id is None or self.channel is None:
            raise RuntimeError("Needed sensor information is missing")

        # odc header
        observations = df_decoded[
            (df_decoded["obstype@hdr"] == self.obstype) &
            (df_decoded["varno@body"] == self.varno) &
            (df_decoded["sensor@hdr"] == self.instrument_id) &
            (df_decoded["satellite_identifier@sat"] == self.satelite_id) &
            (df_decoded["vertco_reference_1@body"] == self.channel) &
            (df_decoded["an_depar@body"] > self.missing)
        ]
        observations = observations.rename(columns={
            'biascorr@body': 'biascrl',
        })
        osize = len(observations)
        extra = {
            "stid": ["NA" for i in range(0,osize)],
        }
        observations = observations.assign(**extra)
        return observations

    def filter_odb_scatt_data(self, df_decoded):

        observations = df_decoded[
            (df_decoded["obstype@hdr"] == self.obstype) &
            (df_decoded["varno@body"] == self.varno) &
            (df_decoded["an_depar@body"] > self.missing)
        ]

        if self.satelite_id is not None:
            observations = df_decoded[
                (df_decoded["satellite_identifier@sat"] == self.satelite_id)
            ]
        osize = len(observations)
        extra = {
            "biascrl": [0.0 for i in range(0,osize)],
            "laf": [0 for i in range(0,osize)],
            "stid": ["NA" for i in range(0,osize)],
        }
        observations = observations.assign(**extra)
        return observations

    def filter_odb_radar_data(self, df_decoded):

        observations = df_decoded[
            (df_decoded["obstype@hdr"] == self.obstype) &
            (df_decoded["varno@body"] == self.varno) &
            (df_decoded["vertco_reference_2@body"] == self.level) &
            (df_decoded["an_depar@body"] > self.missing)
        ]
        if self.codetypes is not None:
            observations = df_decoded[
                (df_decoded["codetype@hdr"].isin(self.codetypes))
            ]
        return observations


def get_odb_data_from_file(odb_file):
    df_decoded = odc.read_odb(odb_file, single=True)
    return df_decoded
