from calendar import c
import pandas as pd
import numpy as np
from google.cloud import bigquery
import pandas_gbq
from google.oauth2 import service_account
import pandas
import requests
import json
from google.cloud import bigquery
import threading
import time
import textwrap
import concurrent.futures

client = bigquery.Client()

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


start_block = '0'
end_block = 'latest'
max_num = 1000

head_list = [
    'TS_FROM', 'TS_TO', 'TLD_1_1', 'TL_1_1', 'TLPD_1_1', 'TLP_1_1', 'TBD_1_1', 'TB_1_1', 'LGR_1_1', 'LGR_APY_1_1', 'LW_1_1', 'LA_1_1', 'LFD_1_1', 'LFC_1_1', 'PFD_1_1', 'PFC_1_1', 'EQB_1_1', 'TXAs_1_1', 'EQF_1_1', 'EQF_1_1_1_2', 'EQF_1_1_2_2', 'EQF_1_1_2_5', 'EQF_1_1_6_1', 'EQF_1_1_6_2', 'EQF_1_1_9_1', 'EQF_1_1_9_2', 'EQF_1_1_10_1', 'EQF_1_1_10_2', 'EQF_1_1_11_1', 'EQF_1_1_12_1', 'EQR_1_1', 'EQR_1_1_1_2', 'EQR_1_1_2_2', 'EQR_1_1_2_5', 'EQR_1_1_6_1', 'EQR_1_1_6_2', 'EQR_1_1_9_1', 'EQR_1_1_9_2', 'EQR_1_1_10_1', 'EQR_1_1_10_2', 'EQR_1_1_11_1', 'EQR_1_1_12_1', 'TXAi_1_1', 'TXAi_1_1_1_2', 'TXAi_1_1_2_2', 'TXAi_1_1_2_5', 'TXAi_1_1_6_1', 'TXAi_1_1_6_2', 'TXAi_1_1_9_1', 'TXAi_1_1_9_2', 'TXAi_1_1_10_1', 'TXAi_1_1_10_2', 'TXAi_1_1_11_1', 'TXAi_1_1_12_1', 'STGM_1_1', 'FROI_1_1', 'TLD_1_2', 'TL_1_2', 'TLPD_1_2', 'TLP_1_2', 'TBD_1_2', 'TB_1_2', 'LGR_1_2', 'LGR_APY_1_2', 'LW_1_2', 'LA_1_2', 'LFD_1_2', 'LFC_1_2', 'PFD_1_2', 'PFC_1_2', 'EQB_1_2', 'TXAs_1_2', 'EQF_1_2', 'EQF_1_2_2_2', 'EQF_1_2_2_5', 'EQF_1_2_6_1', 'EQF_1_2_6_2', 'EQF_1_2_9_1', 'EQF_1_2_9_2', 'EQF_1_2_10_1', 'EQF_1_2_10_2', 'EQF_1_2_11_1', 'EQF_1_2_12_1', 'EQR_1_2', 'EQR_1_2_2_2', 'EQR_1_2_2_5', 'EQR_1_2_6_1', 'EQR_1_2_6_2', 'EQR_1_2_9_1', 'EQR_1_2_9_2', 'EQR_1_2_10_1', 'EQR_1_2_10_2', 'EQR_1_2_11_1', 'EQR_1_2_12_1', 'TXAi_1_2', 'TXAi_1_2_2_2', 'TXAi_1_2_2_5', 'TXAi_1_2_6_1', 'TXAi_1_2_6_2', 'TXAi_1_2_9_1', 'TXAi_1_2_9_2', 'TXAi_1_2_10_1', 'TXAi_1_2_10_2', 'TXAi_1_2_11_1', 'TXAi_1_2_12_1', 'STGM_1_2', 'FROI_1_2', 'TLD_1_13', 'TL_1_13', 'TLPD_1_13', 'TLP_1_13', 'TBD_1_13', 'TB_1_13', 'LGR_1_13', 'LGR_APY_1_13', 'LW_1_13', 'LA_1_13', 'LFD_1_13', 'LFC_1_13', 'PFD_1_13', 'PFC_1_13', 'EQB_1_13', 'TXAs_1_13', 'EQF_1_13', 'EQF_1_13_10_13', 'EQF_1_13_11_13', 'EQR_1_13', 'EQR_1_13_10_13', 'EQR_1_13_11_13', 'TXAi_1_13', 'TXAi_1_13_10_13', 'TXAi_1_13_11_13', 'STGM_1_13', 'FROI_1_13', 'TLD_2_2', 'TL_2_2', 'TLPD_2_2', 'TLP_2_2', 'TBD_2_2', 'TB_2_2', 'LGR_2_2', 'LGR_APY_2_2', 'LW_2_2', 'LA_2_2', 'LFD_2_2', 'LFC_2_2', 'PFD_2_2', 'PFC_2_2', 'EQB_2_2', 'TXAs_2_2', 'EQF_2_2', 'EQF_2_2_1_1', 'EQF_2_2_1_2', 'EQF_2_2_6_1', 'EQF_2_2_6_2', 'EQF_2_2_9_1', 'EQF_2_2_9_2', 'EQF_2_2_10_1', 'EQF_2_2_10_2', 'EQF_2_2_11_1', 'EQF_2_2_12_1', 'EQR_2_2', 'EQR_2_2_1_1', 'EQR_2_2_1_2', 'EQR_2_2_6_1', 'EQR_2_2_6_2', 'EQR_2_2_9_1', 'EQR_2_2_9_2', 'EQR_2_2_10_1', 'EQR_2_2_10_2', 'EQR_2_2_11_1', 'EQR_2_2_12_1', 'TXAi_2_2', 'TXAi_2_2_1_1', 'TXAi_2_2_1_2', 'TXAi_2_2_6_1', 'TXAi_2_2_6_2', 'TXAi_2_2_9_1', 'TXAi_2_2_9_2', 'TXAi_2_2_10_1', 'TXAi_2_2_10_2', 'TXAi_2_2_11_1', 'TXAi_2_2_12_1', 'STGM_2_2', 'FROI_2_2', 'TLD_2_5', 'TL_2_5', 'TLPD_2_5', 'TLP_2_5', 'TBD_2_5', 'TB_2_5', 'LGR_2_5', 'LGR_APY_2_5', 'LW_2_5', 'LA_2_5', 'LFD_2_5', 'LFC_2_5', 'PFD_2_5', 'PFC_2_5', 'EQB_2_5', 'TXAs_2_5', 'EQF_2_5', 'EQF_2_5_1_1', 'EQF_2_5_1_2', 'EQF_2_5_6_1', 'EQF_2_5_6_2', 'EQF_2_5_9_1', 'EQF_2_5_9_2', 'EQF_2_5_10_1', 'EQF_2_5_10_2', 'EQF_2_5_11_1', 'EQF_2_5_12_1', 'EQR_2_5', 'EQR_2_5_1_1', 'EQR_2_5_1_2', 'EQR_2_5_6_1', 'EQR_2_5_6_2', 'EQR_2_5_9_1', 'EQR_2_5_9_2', 'EQR_2_5_10_1', 'EQR_2_5_10_2', 'EQR_2_5_11_1', 'EQR_2_5_12_1', 'TXAi_2_5', 'TXAi_2_5_1_1', 'TXAi_2_5_1_2', 'TXAi_2_5_6_1', 'TXAi_2_5_6_2', 'TXAi_2_5_9_1', 'TXAi_2_5_9_2', 'TXAi_2_5_10_1', 'TXAi_2_5_10_2', 'TXAi_2_5_11_1', 'TXAi_2_5_12_1', 'STGM_2_5', 'FROI_2_5', 'TLD_6_1', 'TL_6_1', 'TLPD_6_1', 'TLP_6_1', 'TBD_6_1', 'TB_6_1', 'LGR_6_1', 'LGR_APY_6_1', 'LW_6_1', 'LA_6_1', 'LFD_6_1', 'LFC_6_1', 'PFD_6_1', 'PFC_6_1', 'EQB_6_1', 'TXAs_6_1', 'EQF_6_1', 'EQF_6_1_1_1', 'EQF_6_1_1_2', 'EQF_6_1_2_2', 'EQF_6_1_2_5', 'EQF_6_1_9_1', 'EQF_6_1_9_2', 'EQF_6_1_10_1', 'EQF_6_1_10_2', 'EQF_6_1_11_1', 'EQF_6_1_12_1', 'EQR_6_1', 'EQR_6_1_1_1', 'EQR_6_1_1_2', 'EQR_6_1_2_2', 'EQR_6_1_2_5', 'EQR_6_1_9_1', 'EQR_6_1_9_2', 'EQR_6_1_10_1', 'EQR_6_1_10_2', 'EQR_6_1_11_1', 'EQR_6_1_12_1', 'TXAi_6_1', 'TXAi_6_1_1_1', 'TXAi_6_1_1_2', 'TXAi_6_1_2_2', 'TXAi_6_1_2_5', 'TXAi_6_1_9_1', 'TXAi_6_1_9_2', 'TXAi_6_1_10_1', 'TXAi_6_1_10_2', 'TXAi_6_1_11_1', 'TXAi_6_1_12_1', 'STGM_6_1', 'FROI_6_1', 'TLD_6_2', 'TL_6_2', 'TLPD_6_2', 'TLP_6_2', 'TBD_6_2', 'TB_6_2', 'LGR_6_2', 'LGR_APY_6_2', 'LW_6_2', 'LA_6_2', 'LFD_6_2', 'LFC_6_2', 'PFD_6_2', 'PFC_6_2', 'EQB_6_2', 'TXAs_6_2', 'EQF_6_2', 'EQF_6_2_1_1', 'EQF_6_2_1_2', 'EQF_6_2_2_2', 'EQF_6_2_2_5', 'EQF_6_2_9_1', 'EQF_6_2_9_2', 'EQF_6_2_10_1', 'EQF_6_2_10_2', 'EQF_6_2_11_1', 'EQF_6_2_12_1', 'EQR_6_2', 'EQR_6_2_1_1', 'EQR_6_2_1_2', 'EQR_6_2_2_2', 'EQR_6_2_2_5', 'EQR_6_2_9_1', 'EQR_6_2_9_2', 'EQR_6_2_10_1', 'EQR_6_2_10_2', 'EQR_6_2_11_1', 'EQR_6_2_12_1', 'TXAi_6_2', 'TXAi_6_2_1_1', 'TXAi_6_2_1_2', 'TXAi_6_2_2_2', 'TXAi_6_2_2_5', 'TXAi_6_2_9_1', 'TXAi_6_2_9_2', 'TXAi_6_2_10_1', 'TXAi_6_2_10_2', 'TXAi_6_2_11_1', 'TXAi_6_2_12_1', 'STGM_6_2', 'FROI_6_2', 'TLD_9_1', 'TL_9_1', 'TLPD_9_1', 'TLP_9_1', 'TBD_9_1', 'TB_9_1', 'LGR_9_1', 'LGR_APY_9_1', 'LW_9_1', 'LA_9_1', 'LFD_9_1', 'LFC_9_1', 'PFD_9_1', 'PFC_9_1', 'EQB_9_1', 'TXAs_9_1', 'EQF_9_1', 'EQF_9_1_1_1', 'EQF_9_1_1_2', 'EQF_9_1_2_2', 'EQF_9_1_2_5', 'EQF_9_1_6_1', 'EQF_9_1_6_2', 'EQF_9_1_10_1', 'EQF_9_1_10_2', 'EQF_9_1_11_1', 'EQF_9_1_12_1', 'EQR_9_1', 'EQR_9_1_1_1', 'EQR_9_1_1_2', 'EQR_9_1_2_2', 'EQR_9_1_2_5', 'EQR_9_1_6_1', 'EQR_9_1_6_2', 'EQR_9_1_10_1', 'EQR_9_1_10_2', 'EQR_9_1_11_1', 'EQR_9_1_12_1', 'TXAi_9_1', 'TXAi_9_1_1_1', 'TXAi_9_1_1_2', 'TXAi_9_1_2_2', 'TXAi_9_1_2_5', 'TXAi_9_1_6_1', 'TXAi_9_1_6_2', 'TXAi_9_1_10_1', 'TXAi_9_1_10_2', 'TXAi_9_1_11_1', 'TXAi_9_1_12_1', 'STGM_9_1', 'FROI_9_1', 'TLD_9_2', 'TL_9_2', 'TLPD_9_2', 'TLP_9_2', 'TBD_9_2', 'TB_9_2', 'LGR_9_2', 'LGR_APY_9_2', 'LW_9_2', 'LA_9_2', 'LFD_9_2', 'LFC_9_2', 'PFD_9_2', 'PFC_9_2', 'EQB_9_2', 'TXAs_9_2', 'EQF_9_2', 'EQF_9_2_1_1', 'EQF_9_2_1_2', 'EQF_9_2_2_2', 'EQF_9_2_2_5', 'EQF_9_2_6_1', 'EQF_9_2_6_2', 'EQF_9_2_10_1', 'EQF_9_2_10_2', 'EQF_9_2_11_1', 'EQF_9_2_12_1', 'EQR_9_2', 'EQR_9_2_1_1', 'EQR_9_2_1_2', 'EQR_9_2_2_2', 'EQR_9_2_2_5', 'EQR_9_2_6_1', 'EQR_9_2_6_2', 'EQR_9_2_10_1', 'EQR_9_2_10_2', 'EQR_9_2_11_1', 'EQR_9_2_12_1', 'TXAi_9_2', 'TXAi_9_2_1_1', 'TXAi_9_2_1_2', 'TXAi_9_2_2_2', 'TXAi_9_2_2_5', 'TXAi_9_2_6_1', 'TXAi_9_2_6_2', 'TXAi_9_2_10_1', 'TXAi_9_2_10_2', 'TXAi_9_2_11_1', 'TXAi_9_2_12_1', 'STGM_9_2', 'FROI_9_2', 'TLD_10_1', 'TL_10_1', 'TLPD_10_1', 'TLP_10_1', 'TBD_10_1', 'TB_10_1', 'LGR_10_1', 'LGR_APY_10_1', 'LW_10_1', 'LA_10_1', 'LFD_10_1', 'LFC_10_1', 'PFD_10_1', 'PFC_10_1', 'EQB_10_1', 'TXAs_10_1', 'EQF_10_1', 'EQF_10_1_1_1', 'EQF_10_1_1_2', 'EQF_10_1_2_2', 'EQF_10_1_2_5', 'EQF_10_1_6_1', 'EQF_10_1_6_2', 'EQF_10_1_9_1', 'EQF_10_1_9_2', 'EQF_10_1_11_1', 'EQF_10_1_12_1', 'EQR_10_1', 'EQR_10_1_1_1', 'EQR_10_1_1_2', 'EQR_10_1_2_2', 'EQR_10_1_2_5', 'EQR_10_1_6_1', 'EQR_10_1_6_2', 'EQR_10_1_9_1', 'EQR_10_1_9_2', 'EQR_10_1_11_1', 'EQR_10_1_12_1', 'TXAi_10_1', 'TXAi_10_1_1_1', 'TXAi_10_1_1_2', 'TXAi_10_1_2_2', 'TXAi_10_1_2_5', 'TXAi_10_1_6_1', 'TXAi_10_1_6_2', 'TXAi_10_1_9_1', 'TXAi_10_1_9_2', 'TXAi_10_1_11_1', 'TXAi_10_1_12_1', 'STGM_10_1', 'FROI_10_1', 'TLD_10_2', 'TL_10_2', 'TLPD_10_2', 'TLP_10_2', 'TBD_10_2', 'TB_10_2', 'LGR_10_2', 'LGR_APY_10_2', 'LW_10_2', 'LA_10_2', 'LFD_10_2', 'LFC_10_2', 'PFD_10_2', 'PFC_10_2', 'EQB_10_2', 'TXAs_10_2', 'EQF_10_2', 'EQF_10_2_1_1', 'EQF_10_2_1_2', 'EQF_10_2_2_2', 'EQF_10_2_2_5', 'EQF_10_2_6_1', 'EQF_10_2_6_2', 'EQF_10_2_9_1', 'EQF_10_2_9_2', 'EQF_10_2_11_1', 'EQF_10_2_12_1', 'EQR_10_2', 'EQR_10_2_1_1', 'EQR_10_2_1_2', 'EQR_10_2_2_2', 'EQR_10_2_2_5', 'EQR_10_2_6_1', 'EQR_10_2_6_2', 'EQR_10_2_9_1', 'EQR_10_2_9_2', 'EQR_10_2_11_1', 'EQR_10_2_12_1', 'TXAi_10_2', 'TXAi_10_2_1_1', 'TXAi_10_2_1_2', 'TXAi_10_2_2_2', 'TXAi_10_2_2_5', 'TXAi_10_2_6_1', 'TXAi_10_2_6_2', 'TXAi_10_2_9_1', 'TXAi_10_2_9_2', 'TXAi_10_2_11_1', 'TXAi_10_2_12_1', 'STGM_10_2', 'FROI_10_2', 'TLD_10_13', 'TL_10_13', 'TLPD_10_13', 'TLP_10_13', 'TBD_10_13', 'TB_10_13', 'LGR_10_13', 'LGR_APY_10_13', 'LW_10_13', 'LA_10_13', 'LFD_10_13', 'LFC_10_13', 'PFD_10_13', 'PFC_10_13', 'EQB_10_13', 'TXAs_10_13', 'EQF_10_13', 'EQF_10_13_1_13', 'EQF_10_13_11_13', 'EQR_10_13', 'EQR_10_13_1_13', 'EQR_10_13_11_13', 'TXAi_10_13', 'TXAi_10_13_1_13', 'TXAi_10_13_11_13', 'STGM_10_13', 'FROI_10_13', 'TLD_11_1', 'TL_11_1', 'TLPD_11_1', 'TLP_11_1', 'TBD_11_1', 'TB_11_1', 'LGR_11_1', 'LGR_APY_11_1', 'LW_11_1', 'LA_11_1', 'LFD_11_1', 'LFC_11_1', 'PFD_11_1', 'PFC_11_1', 'EQB_11_1', 'TXAs_11_1', 'EQF_11_1', 'EQF_11_1_1_1', 'EQF_11_1_1_2', 'EQF_11_1_2_2', 'EQF_11_1_2_5', 'EQF_11_1_6_1', 'EQF_11_1_6_2', 'EQF_11_1_9_1', 'EQF_11_1_9_2', 'EQF_11_1_10_1', 'EQF_11_1_10_2', 'EQF_11_1_12_1', 'EQR_11_1', 'EQR_11_1_1_1', 'EQR_11_1_1_2', 'EQR_11_1_2_2', 'EQR_11_1_2_5', 'EQR_11_1_6_1', 'EQR_11_1_6_2', 'EQR_11_1_9_1', 'EQR_11_1_9_2', 'EQR_11_1_10_1', 'EQR_11_1_10_2', 'EQR_11_1_12_1', 'TXAi_11_1', 'TXAi_11_1_1_1', 'TXAi_11_1_1_2', 'TXAi_11_1_2_2', 'TXAi_11_1_2_5', 'TXAi_11_1_6_1', 'TXAi_11_1_6_2', 'TXAi_11_1_9_1', 'TXAi_11_1_9_2', 'TXAi_11_1_10_1', 'TXAi_11_1_10_2', 'TXAi_11_1_12_1', 'STGM_11_1', 'FROI_11_1', 'TLD_11_13', 'TL_11_13', 'TLPD_11_13', 'TLP_11_13', 'TBD_11_13', 'TB_11_13', 'LGR_11_13', 'LGR_APY_11_13', 'LW_11_13', 'LA_11_13', 'LFD_11_13', 'LFC_11_13', 'PFD_11_13', 'PFC_11_13', 'EQB_11_13', 'TXAs_11_13', 'EQF_11_13', 'EQF_11_13_1_13', 'EQF_11_13_10_13', 'EQR_11_13', 'EQR_11_13_1_13', 'EQR_11_13_10_13', 'TXAi_11_13', 'TXAi_11_13_1_13', 'TXAi_11_13_10_13', 'STGM_11_13', 'FROI_11_13', 'TLD_12_1', 'TL_12_1', 'TLPD_12_1', 'TLP_12_1', 'TBD_12_1', 'TB_12_1', 'LGR_12_1', 'LGR_APY_12_1', 'LW_12_1', 'LA_12_1', 'LFD_12_1', 'LFC_12_1', 'PFD_12_1', 'PFC_12_1', 'EQB_12_1', 'TXAs_12_1', 'EQF_12_1', 'EQF_12_1_1_1', 'EQF_12_1_1_2', 'EQF_12_1_2_2', 'EQF_12_1_2_5', 'EQF_12_1_6_1', 'EQF_12_1_6_2', 'EQF_12_1_9_1', 'EQF_12_1_9_2', 'EQF_12_1_10_1', 'EQF_12_1_10_2', 'EQF_12_1_11_1', 'EQR_12_1', 'EQR_12_1_1_1', 'EQR_12_1_1_2', 'EQR_12_1_2_2', 'EQR_12_1_2_5', 'EQR_12_1_6_1', 'EQR_12_1_6_2', 'EQR_12_1_9_1', 'EQR_12_1_9_2', 'EQR_12_1_10_1', 'EQR_12_1_10_2', 'EQR_12_1_11_1', 'TXAi_12_1', 'TXAi_12_1_1_1', 'TXAi_12_1_1_2', 'TXAi_12_1_2_2', 'TXAi_12_1_2_5', 'TXAi_12_1_6_1', 'TXAi_12_1_6_2', 'TXAi_12_1_9_1', 'TXAi_12_1_9_2', 'TXAi_12_1_10_1', 'TXAi_12_1_10_2', 'TXAi_12_1_11_1', 'STGM_12_1', 'FROI_12_1'
    ]

pool_id = {
    "USDC": 1,
    "USDT": 2,
    "BUSD": 5,
    "SGETH": 13,
}

chain_id = {
    "ethereum": 1,
    "bsc": 2,
    "avalanche": 6,
    "polygon": 9,
    "arbitrum": 10,
    "optimism": 11,
    "fantom": 12,
}

event_topics = {
	'set': '0x545b620a3000f6303b158b321f06b4e95e28a27d70aecac8c6bdac4f48a9f6b3',
	'add': '0x1c482cb20f653d55406cc8aa89ebf482b8603c0ffebcf7e6182ff8ac1849d12d',
	'mint': '0xb4c03061fb5b7fed76389d5af8f2e0ddb09f8c70d1333abbb62582835e10accb',
	'burn': '0x49995e5dd6158cf69ad3e9777c46755a1a826a446c6416992167462dad033b2a',
	'swapremote': '0xfb2b592367452f1c437675bed47f5e1e6c25188c17d7ba01a12eb030bc41ccef',
	'swap': '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f',
	'deposit': '0x90890809c654f11d6e72a28fa60149770a0d11ec6c92319d6ceb2bb0a4ea1a15',
	'withdraw': '0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568',
}

chain_info = {
	'ethereum': {
		'address': {
			'USDC': {
				'poolAddress': "0xdf0770dF86a8034b3EFEf0A1Bb3c889B8332FF56",
				'erc20Address': "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
				'deployBlock': 14403393,
			},
			'USDT': {
				'poolAddress': "0x38EA452219524Bb87e18dE1C24D3bB59510BD783",
				'erc20Address': "0xdAC17F958D2ee523a2206206994597C13D831ec7",
				'deployBlock': 14403402,
			},
			'SGETH': {
				'poolAddress': "0x101816545F6bd2b1076434B54383a1E633390A2E",
				'erc20Address': "0x72E2F4830b9E45d52F80aC08CB2bEC0FeF72eD9c",
				'deployBlock': 15035701,
			},
		},
        'LPStaking': '0xB0D502E938ed5f4df2E681fE6E419ff29631d62b',
		'key': '845JF5MISST352XBEQK6XRS6FJE9ZIFWPB',
		'url': 'https://api.etherscan.io/api'
	},
	'bsc': {
		'address': {
			'USDT': {
				'poolAddress': "0x9aA83081AA06AF7208Dcc7A4cB72C94d057D2cda",
				'erc20Address': "0x55d398326f99059fF775485246999027B3197955",
				'deployBlock': 16135132,
			},
			'BUSD': {
				'poolAddress': "0x98a5737749490856b401DB5Dc27F522fC314A4e1",
				'erc20Address': "0xe9e7cea3dedca5984780bafc599bd69add087d56",
				'deployBlock': 16135136
			},
		},
        'LPStaking': '0x3052A0F6ab15b4AE1df39962d5DdEFacA86DaB47',
		'key': 'RRDA5RRPP8US539A3NNGC5IE8DNTW93V4B',
		'url': 'https://api.bscscan.com/api'
	},
	'avalanche': {
		'address': {
			'USDC': {
				'poolAddress': "0x1205f31718499dBf1fCa446663B532Ef87481fe1",
				'deployBlock': 12219159,
				'erc20Address': "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
			},
			'USDT': {
				'poolAddress': "0x29e38769f23701A2e4A8Ef0492e19dA4604Be62c",
				'deployBlock': 12219171,
				'erc20Address': "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
			},
		},
        'LPStaking': '0x8731d54E9D02c286767d56ac03e8037C07e01e98',
		'key': 'DDJSBNQC91M49F2216CCKMWWWX6EJ4U7DE',
		'url': 'https://api.snowtrace.io/api'
	},
	'polygon': {
		'address': {
			'USDC': {
				'poolAddress': "0x1205f31718499dBf1fCa446663B532Ef87481fe1",
				'deployBlock': 26032726,
				'erc20Address': "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
			},
			'USDT': {
				'poolAddress': "0x29e38769f23701A2e4A8Ef0492e19dA4604Be62c",
				'deployBlock': 26032728,
				'erc20Address': "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
			},
		},
        'LPStaking': '0x8731d54E9D02c286767d56ac03e8037C07e01e98',
		'key': 'HBIW4HP9DBQM8VE4W9MRNMYUZ9E3R18WMB',
		'url': 'https://api.polygonscan.com/api'
	},
	'arbitrum': {
		'address': {
			'USDC': {
				'poolAddress': "0x892785f33CdeE22A30AEF750F285E18c18040c3e",
				'deployBlock': 8041115,
				'erc20Address': "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
			},
			'USDT': {
				'poolAddress': "0xB6CfcF89a7B22988bfC96632aC2A9D6daB60d641",
				'deployBlock': 8041122,
				'erc20Address': "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
			},
			'SGETH': {
				'poolAddress': "0x915A55e36A01285A14f05dE6e81ED9cE89772f8e",
				'deployBlock': 16112680, #recently deployed
				'erc20Address': "0x82CbeCF39bEe528B5476FE6d1550af59a9dB6Fc0",
			},
		},
        'LPStaking': '0xeA8DfEE1898a7e0a59f7527F076106d7e44c2176',
		'key': 'QGB3WYFBTR648MMACKWR217U7I1GA9WRXG',
		'url': 'https://api.arbiscan.io/api',
	},
	'optimism': {
		'address': {
			'USDC': {
				'poolAddress': "0xDecC0c09c3B5f6e92EF4184125D5648a66E35298",
				'deployBlock': 4535509, # Mar-17-2022 07:52:39 AM +UTC
				'erc20Address': "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
			},
			'SGETH': {
				'poolAddress': "0xd22363e3762cA7339569F3d33EADe20127D5F98C",
				'deployBlock': 13332734, # Jun-27-2022 06:23:43 PM +UTC
				'erc20Address': "0xb69c8CBCD90A39D8D3d3ccf0a3E968511C3856A0",
			},
		},
        'LPStaking': '0x4DeA9e918c6289a52cd469cAC652727B7b412Cd2',
		'key': '65D69EGDF9SD9PG7SCUGCRDI2F6NKJD4XI',
		'url': 'https://api-optimistic.etherscan.io/api'
	},
	'fantom': {
		'address':{
			'USDC': {
				'poolAddress': "0x12edeA9cd262006cC3C4E77c90d2CD2DD4b1eb97",
				'deployBlock': 33647195 , # Mar-17-2022 07:53:27 AM +UTC
				'erc20Address': "0x04068da6c83afcfa0e13ba15a6696662335d5b75",
			},
		},
        'LPStaking': '0x224D8Fd7aB6AD4c6eb4611Ce56EF35Dec2277F03',
		'key': 'HU8VD7IF1INHGT8BR2XZH5UC1X58AISN1X',
		'url': 'https://api.ftmscan.com/api'
	}
}

event_list = ['mint', 'burn', 'swap', 'swapremote']
def get_credentials():
    global credentials
    credentials = service_account.Credentials.from_service_account_info(
        {
          "type": "service_account",
          "project_id": "nice-proposal-338510",
          "private_key_id": "6cc6ac641ca1ce66e1f4e1594db599079a33b633",
          "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCmb34zlq7hxxmn\nqYZcbuf0Z4LXKEnKeCc+zUO3DDpaKIcZ03GjTrnEeUzpIhmMPhngN0AxTsgzqA2v\nZqpNoveQT9N7R+5hn8bSAsfbTfqfUNi1Pnn8ksueJ/zyAGLAqFKGZSXgTGK7XRpU\nofdDqWvlBDpfEcYTsackjZzJP3BUV7kqIC3+07ZtfRjL8jZFScwie8foqIcijHlz\nc41reBIRqfOCQQWPzA2CxEiY2l0/Pd1/PERQz1ecd1022ylXBH133oNhvYJCx+zK\ncq/leyJHsIkvdCSfD5gDCcJq3G8tbYcuVqriOmbtB/hRNwHj95JtR7SkJu8gOuBA\nttOUWTaBAgMBAAECggEADH1xIkGhxctxZ0MnaaK1toIuxU2+h+VkxeeVOCnrQR5/\nJlbXVX2QHv30DHTPfQD2Dnz8TMGlmuF23B20vxcb7qZcRQlSCt0jiFlxZ8meJWbl\nnxMICDEjUFmeN7JZYYLgOo9hpxz5tdItVSztx5Pxxwx+WD8XDHL4YPEIUz6hro/i\n1Rr00Hi+1DUZDwLZWtKAQwyLFpWzSdlqm0v5oLiIvfaVLFq8WN9HEFd+xextUaPW\nqWvkBxzVWKS73GsF6fDfyDP4Luj3BYeK756eDOZFXOSCJu6OoDW+1tU97vbmRCCu\ngOgviENvnCovRpBmJOSWU5wTekUYVNlEKe1rUzuTBwKBgQDrC6dkyuj5KuETfOly\nJk98iQKpZN9jok03LtRhCWZDR5Zcp36+XbcWrOpTxgPXrZdP8904FqCfwPyIUYvF\nVj7YxnEBatv8Haqa25xc6aU1cy7gmqaCNopczfv8I0O3h3JjrFMpf8fWaEeBHeXn\ncOesRNb+/JVe2d/+8CbSiDOLOwKBgQC1RftGjmW9W6rnBHu9CHSaP36CEyOV4elv\nrBjDC7untQoQuosizkQOolw8fp/54B6NM4N6YuTyBszZDU8zKa/tE2/mOP8cQ1PL\nT0B+FywVWkle56Z9BWcmwWPl3UxCKnBB4lxRXFMLhUqs3JkQHFKpXBki6S9YUmrT\nIQ+R+CxRcwKBgHRepbbaWzQZzau3WWEBpLL0ppO1dIBAt9gNYGrWm4HN7Jzc9HxN\nq7sXS0DEtdxPfq3AABOn6EiP5LedIAVIqP7saMCZ5mZRTckz15uMthXbfCcJikzH\nsgM2nH59+yXfcnv0sovGTutRX62GXvASTzFUN9mwtkdquWLgBWiQtGxHAoGAaLpY\ntOe6Ac9/Cl79ujWzYBZE2ZODIKnXOuxblfwmW3rtqMKNdftjOG03CsBBXpTMMdnM\nIR4XlXi2SsQ0uKNh/38WTBUr1kFTnQdZhD+Q+XkjIJCHEH3ZnXtnYrsavrNUDcxg\nd6T2WZaVkQ8V40fpvez2nMPJ1aYwD3BvVW7i898CgYBAPKAqWyr5cOyfH5HdndlT\nNPBCOMYsiILWQ3Hc6KeA1s+bGNAk+Pndo6lWyZaslnsrA/3WtFiHARhW2uV5UkIc\nJQneEi5Kp6rZ6MliqEMIoD6//3WHY/up5K1bjTBDYxawHVLoPYe6ZQdRU6x5Uwwk\nRkiAAtPBllcCkeQmgfLYCQ==\n-----END PRIVATE KEY-----\n",
          "client_email": "bigquery-service@nice-proposal-338510.iam.gserviceaccount.com",
          "client_id": "117436675962582309304",
          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
          "token_uri": "https://oauth2.googleapis.com/token",
          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bigquery-service%40nice-proposal-338510.iam.gserviceaccount.com"
        }
    )
def closest(lst, K):
     lst = np.asarray(lst)
     idx = (np.abs(lst - K)).argmin()
     return lst[idx]
def check_value(project_id, table_id, field, value):
    try:
        ### check exist variable
        query = f"SELECT {field} FROM `{table_id}` WHERE {field} = '{value}'"
        # print(query)
        exist_value = pandas_gbq.read_gbq(query, project_id, progress_bar_type=None, credentials = credentials)
        if len(exist_value) > 0:
            return True
        else:
            return False
    except:
        return False
def get_rows_with_max(project_id, table_id, field, cond = ''):
    try:
        query = f"SELECT * FROM `{table_id}` WHERE ({field}) IN ( SELECT MAX({field}) FROM `{table_id}` {cond})"
        rows = pandas_gbq.read_gbq(query, project_id, progress_bar_type=None, credentials = credentials)
        # print(rows)
        return rows
    except:
        return 0
def get_max_id(project_id, table_id):
    query = f"SELECT * FROM `{table_id}` WHERE (id) IN ( SELECT MAX(id) FROM `{table_id}`)"
    try:
        last_rows = pandas_gbq.read_gbq(query, project_id, progress_bar_type=None, credentials = credentials)
        if len(list(last_rows['id'])) == 0:
            return 0
        else:
            return last_rows['id'][0]
    except:
        return 0
def manipulate_tbl_config(var_name = '', value = '', action = 'update', all_flag = False):
    project_id = "nice-proposal-338510"
    table_id = "nice-proposal-338510.stg_all_info.tbl_config"
    field = "var_name"
    last_id = 0
    # pandas_gbq.read_gbq("UPDATE `nice-proposal-338510.stg_all_info.tbl_config` SET value = 'wer234234234' WHERE var_name = 'CHAIN_INFO'", project_id = "nice-proposal-338510", credentials = credentials)
    ## create new variable
    if action == 'create':
        if check_value(project_id, table_id, field, var_name):
            return ('error: exist variable')
        else:
            ### get last id
            last_id = get_max_id(project_id, table_id)
            ### set variable
            df = pandas.DataFrame(
                    {
                        "id": [last_id + 1],
                        "var_name": [var_name],
                        "value": [value],
                    }
                )
            pandas_gbq.to_gbq(df, table_id, project_id=project_id, credentials = credentials, if_exists = 'append')
            return f"success creating variable"
            
    ## UPDATE dataset.StockDetails SET color = ‘Mystic Green’ WHERE product=’SG Note 20 Ultra’
    elif action == 'update':
        if not check_value(project_id, table_id, field, var_name):
            return "error: no variable or no table"
        else:
            try:
                df = pandas_gbq.read_gbq(f"UPDATE `{table_id}` SET value = '{value}' WHERE var_name = '{var_name}'", project_id, progress_bar_type=None, credentials = credentials)
                return "success changing variable"
            except:
                return "server error"
    elif action == 'delete':
        if all_flag:
            try:
                df = pandas_gbq.read_gbq(f"DELETE FROM `{table_id}` WHERE true", project_id, progress_bar_type=None, credentials = credentials)
                return "success deleting all variables"
            except:
                return "server error"
        else:
            if not check_value(project_id, table_id, field, var_name):
                return "error: no variable or no table"
            else:
                try:
                    df = pandas_gbq.read_gbq(f"DELETE FROM `{table_id}` WHERE var_name = '{var_name}'", project_id, progress_bar_type=None, credentials = credentials)
                    return "success deleting variable"
                except:
                    return "server error"
    elif action == 'select':
        if all_flag:
            try:
                df = pandas_gbq.read_gbq(f"SELECT * FROM `{table_id}`", project_id, progress_bar_type=None, credentials = credentials)
                return df
            except:
                return "server error"
        else:
            if not check_value(project_id, table_id, field, var_name):
                return "error: no variable or no table"
            else:
                try:
                    df = pandas_gbq.read_gbq(f"SELECT * FROM `{table_id}` WHERE var_name = '{var_name}'", project_id, progress_bar_type=None, credentials = credentials)
                    return df
                except:
                    return "server error"
    else:
        return "arg3 error (insert create, select, update, delete)"

def fetch_pool_events(project_id, chain):
    decimals = 10 ** 6
    # print(project_id, table_id, chain)
    # table_id_list = []
    from_block = {}
    prev_rows = {}
    for pool in chain_info[chain]['address']:
        for i in range(4):
            # table_id_list.append(f'{project_id}.stg_all_info.tbl_{chain}_{pool}_{event_list[i]}')
            try:
                prev_rows[pool + '_' + event_list[i]] = get_rows_with_max(project_id, f'{project_id}.stg_all_info.tbl_{chain}_{pool}', 'block_no', f'WHERE event_type = "{event_list[i]}"')
                from_block[pool + '_' + event_list[i]] = prev_rows[pool + '_' + event_list[i]]['block_no'][0]
            except:
                from_block[pool + '_' + event_list[i]] = 0
                prev_rows[pool + '_' + event_list[i]] = {}
                prev_rows[pool + '_' + event_list[i]]['block_no'] = []
                # pass
    # print(chain, prev_rows)

    resp = {}
    for pool in chain_info[chain]['address']:
        for i in range(4):
            resp[pool + '_' + str(i)] = (requests.get(url=chain_info[chain]['url'], params={
                'module': 'logs',
                'action': 'getLogs',
                'fromBlock': from_block[pool + '_' + event_list[i]],
                'toBlock': 'latest',
                'topic0': event_topics[event_list[i]],
                'apikey': chain_info[chain]['key'],
                'address': chain_info[chain]['address'][pool]['poolAddress']
            }).json()['result'])
    
    # print(resp[0][:1])
    ## decode logs

    for pool in chain_info[chain]['address']:
        block_no = []
        time_stamp = []
        tx = []
        event_type = []
        address = []
        amount_lp = []
        amount_sd = []
        mint_fee = []
        dst_fee = []
        eq_reward = []
        eq_fee = []
        protocol_fee = []
        lp_fee = []
        dst_chain = []
        dst_pool = []
        for i in range(4):
            event = event_list[i]
            for row in resp[f'{pool}_{i}']:
                equal_tx_flag = True
                # print(row ,resp[pool_event])

                ## check events logs with equal tx
                for idx, block in enumerate(prev_rows[pool + '_' + event_list[i]]['block_no']):
                    if prev_rows[pool + '_' + event_list[i]]['tx'][idx] == row['transactionHash']:
                        equal_tx_flag = False
                        # print(chain, pool, prev_rows[pool + '_' + event_list[i]]['tx'][idx], row['transactionHash'])
                if equal_tx_flag:
                    block_no.append(float(eval(row['blockNumber'])))
                    time_stamp.append(float(eval(row['timeStamp'])))
                    tx.append(row['transactionHash'])
                    td_str = ''.join(row['topics']) + row['data']
                    td_str = td_str.replace('0x', '')
                    td_list = textwrap.wrap(td_str, 64)
                    event_type.append(event)
                    if event == 'mint':
                        address.append('0x' + td_list[1][24:])
                        amount_lp.append(float(eval('0x' + td_list[2])) / decimals)
                        amount_sd.append(float(eval('0x' + td_list[3])) / decimals)
                        mint_fee.append(float(eval('0x' + td_list[4])) / decimals)
                        dst_fee.append(0)
                        eq_reward.append(0)
                        eq_fee.append(0)
                        protocol_fee.append(0)
                        lp_fee.append(0)
                        dst_chain.append(0)
                        dst_pool.append(0)
                    if event == 'burn':
                        address.append('0x' + td_list[1][24:])
                        amount_lp.append(float(eval('0x' + td_list[2])) / decimals)
                        amount_sd.append(float(eval('0x' + td_list[3])) / decimals)
                        mint_fee.append(0)
                        dst_fee.append(0)
                        eq_reward.append(0)
                        eq_fee.append(0)
                        protocol_fee.append(0)
                        lp_fee.append(0)
                        dst_chain.append(0)
                        dst_pool.append(0)
                    if event == 'swap':
                        dst_chain.append(int(eval('0x' + td_list[1])))
                        dst_pool.append(int(eval('0x' + td_list[2])))
                        address.append('0x' + td_list[3][24:])
                        amount_sd.append(float(eval('0x' + td_list[4])) / decimals)
                        eq_reward.append(float(eval('0x' + td_list[5])) / decimals)
                        eq_fee.append(float(eval('0x' + td_list[6])) / decimals)
                        protocol_fee.append(float(eval('0x' + td_list[7])) / decimals)
                        lp_fee.append(float(eval('0x' + td_list[8])) / decimals)
                        amount_lp.append(0)
                        mint_fee.append(0)
                        dst_fee.append(0)
                    if event == 'swapremote':
                        address.append('0x' + td_list[1][24:])
                        amount_sd.append(float(eval('0x' + td_list[2])) / decimals)
                        protocol_fee.append(float(eval('0x' + td_list[3])) / decimals)
                        dst_fee.append(float(eval('0x' + td_list[4])) / decimals)
                        amount_lp.append(0)
                        eq_fee.append(0)
                        eq_reward.append(0)
                        dst_chain.append(0)
                        dst_pool.append(0)
                        mint_fee.append(0)
                        lp_fee.append(0)

        df = pandas.DataFrame(
            {
                'block_no': block_no,
                'timestamp': time_stamp,
                'tx': tx,
                'event_type': event_type,
                'address': address,
                'amount_lp': amount_lp,
                'amount_sd': amount_sd,
                'mint_fee': mint_fee,
                'dst_fee': dst_fee,
                'eq_reward': eq_reward,
                'eq_fee': eq_fee,
                'protocol_fee': protocol_fee,
                'lp_fee': lp_fee,
                'dst_chain': dst_chain,
                'dst_pool': dst_pool,
            }
        )
        # df['block_no'] = df['block_no'].astype('string') 
        # df.to_csv(f'{project_id}.stg_all_info.tbl_{chain}_{pool_event}',sep='\t',encoding='utf-8')
        # temp_csv_string = df.to_csv(sep='\t',encoding='utf-8', index=False)
        # temp_csv_string_IO = stringIO(temp_csv_string)
        # new_df = pd.read_csv(temp_csv_string_IO, sep="\t")
        try:
            pandas_gbq.to_gbq(df, f'{project_id}.stg_all_info.tbl_{chain}_{pool}', project_id=project_id, credentials=credentials, if_exists='append', table_schema=[
                {'name': 'block_no', 'type': 'float64'},
                {'name': 'timestamp', 'type': 'float64'},
                {'name': 'tx', 'type': 'string'},
                {'name': 'event_type', 'type': 'string'},
                {'name': 'address', 'type': 'string'},
                {'name': 'amount_lp', 'type': 'float64'},
                {'name': 'amount_sd', 'type': 'float64'},
                {'name': 'mint_fee', 'type': 'float64'},
                {'name': 'dst_fee', 'type': 'float64'},
                {'name': 'eq_reward', 'type': 'float64'},
                {'name': 'eq_fee', 'type': 'float64'},
                {'name': 'protocol_fee', 'type': 'float64'},
                {'name': 'lp_fee', 'type': 'float64'},
                {'name': 'dst_chain', 'type': 'float64'},
                {'name': 'dst_pool', 'type': 'float64'}
                ])
        except:
            print('error to_gbq')
        
def delete_table(project_id, table_id):
    query = f"DROP TABLE `{table_id}`"
    # print(query)
    try:
        pandas_gbq.read_gbq(query, progress_bar_type=None, credentials = credentials)
    except:
        pass
def get_data(project_id, table_id, cond = ''):
    query = f"SELECT * FROM {project_id}.{table_id} {cond}"
    result = pandas_gbq.read_gbq(query, progress_bar_type=None, credentials=credentials)
    return (result)
def process_data():
    project_id = "nice-proposal-338510"
    tbl_stablecoin_id = "stg_all_info.tbl_stablecoin"
    df_obj = {}
    tbl_schema = []
    ts_from_list = []
    ts_to_list = []
    predata = {}
    max_time_list = {}
    last_time = 0
    last_time_list = []

    for head in head_list:
        if head == 'TS_FROM' or head == 'TS_TO':
            tbl_schema.append({'name': head, 'type': 'int64'})
        else:
            tbl_schema.append({'name': head, 'type': 'float64'})
            
    try:
        client.get_table(project_id + '.' + tbl_stablecoin_id)  # Make an API request.
        print("Table {} already exists.".format(tbl_stablecoin_id))
    except NotFound:
        print("Table {} is not found.".format(tbl_stablecoin_id))
        for head in head_list:
            ## set dataframe
            df_obj[head] = [0]
            if 'LGR' in head:
                df_obj[head] = [1]
            if head == 'TS_FROM':
                df_obj[head] = [1647446400]  ## 2022-03-17 00:00:00
            if head == 'TS_TO':
                df_obj[head] = [1647446400 + 900]

            ## set tbl_schema
            if head == "TS_FROM" or head == "TS_TO":
                tbl_schema.append({'name': head, 'type': 'int64'})
            else:
                tbl_schema.append({'name': head, 'type': 'float64'})
        df = pandas.DataFrame(df_obj)
        # print(df)
        pandas_gbq.to_gbq(df, tbl_stablecoin_id, project_id=project_id, credentials=credentials, if_exists='fail', table_schema=tbl_schema)

    # delete_table(project_id, tbl_stablecoin_id)
    # return
    
    ## get prev data

    prev_all_fields = get_rows_with_max(project_id, tbl_stablecoin_id, 'TS_FROM')
    prev_TS_TO = prev_all_fields['TS_TO'][0]
    # print(prev_TS_TO)

    ## get data to be processed (data is in after prev_TS_FROM)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for chain in chain_id:
            for pool in chain_info[chain]['address']:
                exec(f"{chain + '_' + pool} = executor.submit(get_data, '{project_id}', 'stg_all_info.tbl_{chain}_{pool}', 'WHERE timestamp >= {prev_TS_TO} order by timestamp')")
                for event in event_list:
                    exec(f"{chain + '_' + pool + '_' + event} = executor.submit(get_data, '{project_id}', 'stg_all_info.tbl_{chain}_{pool}', 'WHERE timestamp >= {prev_TS_TO} and event_type = \"{event}\"')")
                
        for chain in chain_id:
            for pool in chain_info[chain]['address']:
                exec(f"predata['{chain + '_' + pool}'] = {chain + '_' + pool}.result()")
                for event in event_list:
                    exec(f"max_time_list['{chain + '_' + pool + '_' + event}'] = {chain + '_' + pool + '_' + event}.result()")

    for chain in chain_id:
        for pool in chain_info[chain]['address']:
            for event in event_list:
                last_time_list.append(max(max_time_list[chain + '_' + pool + '_' + event]['timestamp']))
    
    last_time = min(last_time_list)
    # print(last_time)
    # print(predata)
    while (prev_TS_TO + 900) < last_time:
        ts_from_list.append(prev_TS_TO)
        prev_TS_TO += 900
        ts_to_list.append(prev_TS_TO)
    last_time = ts_to_list[-1]
    # print(ts_from_list[0], ts_to_list[0], ts_from_list[-1], ts_to_list[-1])
    # print(len(head_list))
    all_fields_dataset = {}
    for head in head_list:
        all_fields_dataset[head] = []
        for idx, ts_from in enumerate(ts_from_list):
            if head == 'TS_FROM':
                all_fields_dataset[head].append(ts_from)
            elif head == 'TS_TO':
                all_fields_dataset[head].append(ts_from + 900)
            else:
                if 'LGR' in head:
                    all_fields_dataset[head].append(1)
                else:
                    all_fields_dataset[head].append(0)
    # print(len(all_fields_dataset), len(all_fields_dataset['LGR_1_1']), len(all_fields_dataset['EQR_1_1_1_2']))
    for chain_pool in predata:
        cid = chain_id[chain_pool.split('_')[0]]
        pid = pool_id[chain_pool.split('_')[1]]
        cp_key =f"{cid}_{pid}"
        # print(cp_key)
        prev_ts = prev_all_fields['TS_TO'][0]
        # print(prev_ts)
        new_flag = True
        new_ts_flag = False
        for idx, timestamp in enumerate(predata[chain_pool]['timestamp']):
            if timestamp < last_time:
                # break
                ts_from = closest(ts_from_list, timestamp)
                if ts_from > timestamp:
                    ts_from -= 900
                # print(ts_from)
                if timestamp >= prev_ts:
                    new_ts_flag = True
                    # prev_ts = ts_from + 900
                # for head in [x for x in head_list if x.split['_'][1] == cid and x.split['_'][2] == pid]:
                event_type = predata[chain_pool]['event_type'][idx]
                if new_flag:
                    while ts_from >= prev_ts:
                        all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(prev_ts)] = prev_all_fields[f'TL_{cp_key}'][0]
                        all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(prev_ts)] = prev_all_fields[f'TLP_{cp_key}'][0]
                        all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(prev_ts)] = prev_all_fields[f'TB_{cp_key}'][0]
                        all_fields_dataset[f'LFC_{cp_key}'][ts_from_list.index(prev_ts)] = prev_all_fields[f'LFC_{cp_key}'][0]
                        all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(prev_ts)] = prev_all_fields[f'PFC_{cp_key}'][0]
                        # all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        # all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        # all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        # all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        prev_ts += 900
                    new_flag = False
                elif new_ts_flag:
                    while ts_from >= prev_ts:
                        all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        prev_ts += 900
                        new_ts_flag = False
                if event_type == 'mint':
                    all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TLD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_lp'][idx]
                    all_fields_dataset[f'TLPD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_lp'][idx]
                    all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TBD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'LA_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                if event_type == 'burn':
                    all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TLD_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_lp'][idx]
                    all_fields_dataset[f'TLPD_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_lp'][idx]
                    all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TBD_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'LW_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                if event_type == 'swapremote':
                    all_fields_dataset[f'TLD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['dst_fee'][idx]
                    newTL = all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)] + predata[chain_pool]['dst_fee'][idx]
                    if all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)] != 0:
                        all_fields_dataset[f'LGR_{cp_key}'][ts_from_list.index(ts_from)] *= newTL / all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)]
                    all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)] = newTL
                    all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TBD_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'LFC_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['lp_fee'][idx]
                    all_fields_dataset[f'LFD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['lp_fee'][idx]
                    all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['protocol_fee'][idx]
                    all_fields_dataset[f'PFD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['protocol_fee'][idx]
                    all_fields_dataset[f'TXAs_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                if event_type == 'swap':
                    all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TBD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'EQF_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['eq_fee'][idx]
                    all_fields_dataset[f'EQR_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['eq_reward'][idx]
                    all_fields_dataset[f'TXAi_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    if len(str(int(predata[chain_pool]['dst_chain'][idx]))) != 3:
                        dst_cp_key = str(int(predata[chain_pool]['dst_chain'][idx])) + '_' + str(int(predata[chain_pool]['dst_pool'][idx]))
                    else:
                        if str(int(predata[chain_pool]['dst_chain'][idx]))[1] == '0':
                            dst_cp_key = str(int(predata[chain_pool]['dst_chain'][idx]))[-1] + '_' + str(int(predata[chain_pool]['dst_pool'][idx]))
                        else:
                            dst_cp_key = str(int(predata[chain_pool]['dst_chain'][idx]))[1:] + '_' + str(int(predata[chain_pool]['dst_pool'][idx]))
                    all_fields_dataset[f'EQF_{cp_key}_{dst_cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['eq_fee'][idx]
                    all_fields_dataset[f'EQR_{cp_key}_{dst_cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['eq_reward'][idx]
                    all_fields_dataset[f'TXAi_{cp_key}_{dst_cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]

            # print(predata[chain_pool]['timestamp'][idx])
            
            # break

    # for chain in chain_id:
    #     for pool in chain_info[chain]['address']:
    #         predata[chain + pool] = get_data(project_id, f'stg_all_info.tbl_{chain}_{pool}')
    # print(predata)

    df = pandas.DataFrame(all_fields_dataset)
    # print(df)
    pandas_gbq.to_gbq(df, tbl_stablecoin_id, project_id=project_id, credentials=credentials, if_exists='append', table_schema=tbl_schema)

def main(req1, req2):
    ## start time
    start_time = time.time()

    get_credentials()


    ## fetch pool events logs using multithread
    # for chain in chain_id:
    #     exec(f"{chain}_thread = threading.Thread(target=fetch_pool_events, args = ('nice-proposal-338510', '{chain}'))")
    # for chain in chain_id:
    #     exec(f"{chain}_thread.start()")
    # for chain in chain_id:
    #     exec(f"{chain}_thread.join()")
        

    ## fetch pool events logs using only main thread
    # for chain in chain_id:
    #     fetch_pool_events(project_id, chain)

    ## DROP TABLE using multi thread
    # for chain in chain_id:
    #     for pool in pool_id:
    #         exec(f"{chain}_{pool}_thread = threading.Thread(target=delete_table, args =('nice-proposal-338510', 'nice-proposal-338510.stg_all_info.tbl_{chain}_{pool}'))")
    
    # for chain in chain_id:
    #     for pool in pool_id:
    #         exec(f"{chain}_{pool}_thread.start()")
    
    # for chain in chain_id:
    #     for pool in pool_id:
    #         exec(f"{chain}_{pool}_thread.join()")

    ## 
    process_data()

    ## end time
    print("--- %s seconds ---" % (time.time() - start_time))
# main()
