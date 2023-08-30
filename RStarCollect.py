#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 14:56:52 2023

@author: diegoalvarez
"""

import pandas as pd

import warnings
warnings.simplefilter(
    action = "ignore",
    category = FutureWarning)

class RStarCollect:

    def __init__(self):

        self.htmls = {
            "lw_real_time": "https://www.newyorkfed.org/medialibrary/media/research/economists/williams/data/Laubach_Williams_real_time_estimates.xlsx",
            "lw_current_model": "https://www.newyorkfed.org/medialibrary/media/research/economists/williams/data/Laubach_Williams_current_estimates.xlsx",
            "hlw_real_time": "https://www.newyorkfed.org/medialibrary/media/research/economists/williams/data/Holston_Laubach_Williams_real_time_estimates.xlsx",
            "hlw_current_model": "https://www.newyorkfed.org/medialibrary/media/research/economists/williams/data/Holston_Laubach_Williams_current_estimates.xlsx"}

    def collect_lw_current_model(self):

        print("[INFO] Working LW Current Model")
        df_dict = (pd.read_excel(
            io = self.htmls["lw_current_model"],
            header = 5,
            sheet_name = None))

        return df_dict

    def collect_lw_real_time(self):

        print("[INFO] Working on LW Real Time Data Collection")
        df_dict = (pd.read_excel(
            io = self.htmls["lw_real_time"],
            header = 5,
            sheet_name = None))

        df_raw = self._collect_lw_real_time_dict(df_dict)
        self.lw_real_time_df = df_raw
        self.lw_real_time_df.to_parquet("lw_real_time.parquet")

    # need to rewrite so that I have one_sided and two sided in correctly
    def _collect_lw_real_time_dict(self, df_dict: dict, verbose = False) -> pd.DataFrame:

        df_combined = pd.DataFrame()
        for quarter in df_dict:

            if verbose == True: print("[INFO] Cleaning", quarter)
            df_tmp = (df_dict[
                quarter].
                melt(id_vars = "Date").
                dropna().
                assign(
                    variable = lambda x: x.variable.str.replace(".1", "_one_tail").str.replace(".2", "_two_tail"),
                    quarter = lambda x: quarter))

            df_combined = pd.concat([df_combined, df_tmp])

        return df_combined