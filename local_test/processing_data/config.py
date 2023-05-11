
head_list = [
    'TS_FROM', 'TS_TO', 'TLD_1_1', 'TL_1_1', 'TLPD_1_1', 'TLP_1_1', 'TBD_1_1', 'TB_1_1', 'LGR_1_1', 'LGR_APY_1_1', 'LW_1_1', 'LA_1_1', 'LFD_1_1', 'LFC_1_1', 'PFD_1_1', 'PFC_1_1', 'EQB_1_1', 'TXAs_1_1', 'EQF_1_1', 'EQF_1_1_1_2', 'EQF_1_1_2_2', 'EQF_1_1_2_5', 'EQF_1_1_6_1', 'EQF_1_1_6_2', 'EQF_1_1_9_1', 'EQF_1_1_9_2', 'EQF_1_1_10_1', 'EQF_1_1_10_2', 'EQF_1_1_11_1', 'EQF_1_1_12_1', 'EQR_1_1', 'EQR_1_1_1_2', 'EQR_1_1_2_2', 'EQR_1_1_2_5', 'EQR_1_1_6_1', 'EQR_1_1_6_2', 'EQR_1_1_9_1', 'EQR_1_1_9_2', 'EQR_1_1_10_1', 'EQR_1_1_10_2', 'EQR_1_1_11_1', 'EQR_1_1_12_1', 'TXAi_1_1', 'TXAi_1_1_1_2', 'TXAi_1_1_2_2', 'TXAi_1_1_2_5', 'TXAi_1_1_6_1', 'TXAi_1_1_6_2', 'TXAi_1_1_9_1', 'TXAi_1_1_9_2', 'TXAi_1_1_10_1', 'TXAi_1_1_10_2', 'TXAi_1_1_11_1', 'TXAi_1_1_12_1', 'STGM_1_1', 'FROI_1_1', 'TLD_1_2', 'TL_1_2', 'TLPD_1_2', 'TLP_1_2', 'TBD_1_2', 'TB_1_2', 'LGR_1_2', 'LGR_APY_1_2', 'LW_1_2', 'LA_1_2', 'LFD_1_2', 'LFC_1_2', 'PFD_1_2', 'PFC_1_2', 'EQB_1_2', 'TXAs_1_2', 'EQF_1_2', 'EQF_1_2_2_2', 'EQF_1_2_2_5', 'EQF_1_2_6_1', 'EQF_1_2_6_2', 'EQF_1_2_9_1', 'EQF_1_2_9_2', 'EQF_1_2_10_1', 'EQF_1_2_10_2', 'EQF_1_2_11_1', 'EQF_1_2_12_1', 'EQR_1_2', 'EQR_1_2_2_2', 'EQR_1_2_2_5', 'EQR_1_2_6_1', 'EQR_1_2_6_2', 'EQR_1_2_9_1', 'EQR_1_2_9_2', 'EQR_1_2_10_1', 'EQR_1_2_10_2', 'EQR_1_2_11_1', 'EQR_1_2_12_1', 'TXAi_1_2', 'TXAi_1_2_2_2', 'TXAi_1_2_2_5', 'TXAi_1_2_6_1', 'TXAi_1_2_6_2', 'TXAi_1_2_9_1', 'TXAi_1_2_9_2', 'TXAi_1_2_10_1', 'TXAi_1_2_10_2', 'TXAi_1_2_11_1', 'TXAi_1_2_12_1', 'STGM_1_2', 'FROI_1_2', 'TLD_1_13', 'TL_1_13', 'TLPD_1_13', 'TLP_1_13', 'TBD_1_13', 'TB_1_13', 'LGR_1_13', 'LGR_APY_1_13', 'LW_1_13', 'LA_1_13', 'LFD_1_13', 'LFC_1_13', 'PFD_1_13', 'PFC_1_13', 'EQB_1_13', 'TXAs_1_13', 'EQF_1_13', 'EQF_1_13_10_13', 'EQF_1_13_11_13', 'EQR_1_13', 'EQR_1_13_10_13', 'EQR_1_13_11_13', 'TXAi_1_13', 'TXAi_1_13_10_13', 'TXAi_1_13_11_13', 'STGM_1_13', 'FROI_1_13', 'TLD_2_2', 'TL_2_2', 'TLPD_2_2', 'TLP_2_2', 'TBD_2_2', 'TB_2_2', 'LGR_2_2', 'LGR_APY_2_2', 'LW_2_2', 'LA_2_2', 'LFD_2_2', 'LFC_2_2', 'PFD_2_2', 'PFC_2_2', 'EQB_2_2', 'TXAs_2_2', 'EQF_2_2', 'EQF_2_2_1_1', 'EQF_2_2_1_2', 'EQF_2_2_6_1', 'EQF_2_2_6_2', 'EQF_2_2_9_1', 'EQF_2_2_9_2', 'EQF_2_2_10_1', 'EQF_2_2_10_2', 'EQF_2_2_11_1', 'EQF_2_2_12_1', 'EQR_2_2', 'EQR_2_2_1_1', 'EQR_2_2_1_2', 'EQR_2_2_6_1', 'EQR_2_2_6_2', 'EQR_2_2_9_1', 'EQR_2_2_9_2', 'EQR_2_2_10_1', 'EQR_2_2_10_2', 'EQR_2_2_11_1', 'EQR_2_2_12_1', 'TXAi_2_2', 'TXAi_2_2_1_1', 'TXAi_2_2_1_2', 'TXAi_2_2_6_1', 'TXAi_2_2_6_2', 'TXAi_2_2_9_1', 'TXAi_2_2_9_2', 'TXAi_2_2_10_1', 'TXAi_2_2_10_2', 'TXAi_2_2_11_1', 'TXAi_2_2_12_1', 'STGM_2_2', 'FROI_2_2', 'TLD_2_5', 'TL_2_5', 'TLPD_2_5', 'TLP_2_5', 'TBD_2_5', 'TB_2_5', 'LGR_2_5', 'LGR_APY_2_5', 'LW_2_5', 'LA_2_5', 'LFD_2_5', 'LFC_2_5', 'PFD_2_5', 'PFC_2_5', 'EQB_2_5', 'TXAs_2_5', 'EQF_2_5', 'EQF_2_5_1_1', 'EQF_2_5_1_2', 'EQF_2_5_6_1', 'EQF_2_5_6_2', 'EQF_2_5_9_1', 'EQF_2_5_9_2', 'EQF_2_5_10_1', 'EQF_2_5_10_2', 'EQF_2_5_11_1', 'EQF_2_5_12_1', 'EQR_2_5', 'EQR_2_5_1_1', 'EQR_2_5_1_2', 'EQR_2_5_6_1', 'EQR_2_5_6_2', 'EQR_2_5_9_1', 'EQR_2_5_9_2', 'EQR_2_5_10_1', 'EQR_2_5_10_2', 'EQR_2_5_11_1', 'EQR_2_5_12_1', 'TXAi_2_5', 'TXAi_2_5_1_1', 'TXAi_2_5_1_2', 'TXAi_2_5_6_1', 'TXAi_2_5_6_2', 'TXAi_2_5_9_1', 'TXAi_2_5_9_2', 'TXAi_2_5_10_1', 'TXAi_2_5_10_2', 'TXAi_2_5_11_1', 'TXAi_2_5_12_1', 'STGM_2_5', 'FROI_2_5', 'TLD_6_1', 'TL_6_1', 'TLPD_6_1', 'TLP_6_1', 'TBD_6_1', 'TB_6_1', 'LGR_6_1', 'LGR_APY_6_1', 'LW_6_1', 'LA_6_1', 'LFD_6_1', 'LFC_6_1', 'PFD_6_1', 'PFC_6_1', 'EQB_6_1', 'TXAs_6_1', 'EQF_6_1', 'EQF_6_1_1_1', 'EQF_6_1_1_2', 'EQF_6_1_2_2', 'EQF_6_1_2_5', 'EQF_6_1_9_1', 'EQF_6_1_9_2', 'EQF_6_1_10_1', 'EQF_6_1_10_2', 'EQF_6_1_11_1', 'EQF_6_1_12_1', 'EQR_6_1', 'EQR_6_1_1_1', 'EQR_6_1_1_2', 'EQR_6_1_2_2', 'EQR_6_1_2_5', 'EQR_6_1_9_1', 'EQR_6_1_9_2', 'EQR_6_1_10_1', 'EQR_6_1_10_2', 'EQR_6_1_11_1', 'EQR_6_1_12_1', 'TXAi_6_1', 'TXAi_6_1_1_1', 'TXAi_6_1_1_2', 'TXAi_6_1_2_2', 'TXAi_6_1_2_5', 'TXAi_6_1_9_1', 'TXAi_6_1_9_2', 'TXAi_6_1_10_1', 'TXAi_6_1_10_2', 'TXAi_6_1_11_1', 'TXAi_6_1_12_1', 'STGM_6_1', 'FROI_6_1', 'TLD_6_2', 'TL_6_2', 'TLPD_6_2', 'TLP_6_2', 'TBD_6_2', 'TB_6_2', 'LGR_6_2', 'LGR_APY_6_2', 'LW_6_2', 'LA_6_2', 'LFD_6_2', 'LFC_6_2', 'PFD_6_2', 'PFC_6_2', 'EQB_6_2', 'TXAs_6_2', 'EQF_6_2', 'EQF_6_2_1_1', 'EQF_6_2_1_2', 'EQF_6_2_2_2', 'EQF_6_2_2_5', 'EQF_6_2_9_1', 'EQF_6_2_9_2', 'EQF_6_2_10_1', 'EQF_6_2_10_2', 'EQF_6_2_11_1', 'EQF_6_2_12_1', 'EQR_6_2', 'EQR_6_2_1_1', 'EQR_6_2_1_2', 'EQR_6_2_2_2', 'EQR_6_2_2_5', 'EQR_6_2_9_1', 'EQR_6_2_9_2', 'EQR_6_2_10_1', 'EQR_6_2_10_2', 'EQR_6_2_11_1', 'EQR_6_2_12_1', 'TXAi_6_2', 'TXAi_6_2_1_1', 'TXAi_6_2_1_2', 'TXAi_6_2_2_2', 'TXAi_6_2_2_5', 'TXAi_6_2_9_1', 'TXAi_6_2_9_2', 'TXAi_6_2_10_1', 'TXAi_6_2_10_2', 'TXAi_6_2_11_1', 'TXAi_6_2_12_1', 'STGM_6_2', 'FROI_6_2', 'TLD_9_1', 'TL_9_1', 'TLPD_9_1', 'TLP_9_1', 'TBD_9_1', 'TB_9_1', 'LGR_9_1', 'LGR_APY_9_1', 'LW_9_1', 'LA_9_1', 'LFD_9_1', 'LFC_9_1', 'PFD_9_1', 'PFC_9_1', 'EQB_9_1', 'TXAs_9_1', 'EQF_9_1', 'EQF_9_1_1_1', 'EQF_9_1_1_2', 'EQF_9_1_2_2', 'EQF_9_1_2_5', 'EQF_9_1_6_1', 'EQF_9_1_6_2', 'EQF_9_1_10_1', 'EQF_9_1_10_2', 'EQF_9_1_11_1', 'EQF_9_1_12_1', 'EQR_9_1', 'EQR_9_1_1_1', 'EQR_9_1_1_2', 'EQR_9_1_2_2', 'EQR_9_1_2_5', 'EQR_9_1_6_1', 'EQR_9_1_6_2', 'EQR_9_1_10_1', 'EQR_9_1_10_2', 'EQR_9_1_11_1', 'EQR_9_1_12_1', 'TXAi_9_1', 'TXAi_9_1_1_1', 'TXAi_9_1_1_2', 'TXAi_9_1_2_2', 'TXAi_9_1_2_5', 'TXAi_9_1_6_1', 'TXAi_9_1_6_2', 'TXAi_9_1_10_1', 'TXAi_9_1_10_2', 'TXAi_9_1_11_1', 'TXAi_9_1_12_1', 'STGM_9_1', 'FROI_9_1', 'TLD_9_2', 'TL_9_2', 'TLPD_9_2', 'TLP_9_2', 'TBD_9_2', 'TB_9_2', 'LGR_9_2', 'LGR_APY_9_2', 'LW_9_2', 'LA_9_2', 'LFD_9_2', 'LFC_9_2', 'PFD_9_2', 'PFC_9_2', 'EQB_9_2', 'TXAs_9_2', 'EQF_9_2', 'EQF_9_2_1_1', 'EQF_9_2_1_2', 'EQF_9_2_2_2', 'EQF_9_2_2_5', 'EQF_9_2_6_1', 'EQF_9_2_6_2', 'EQF_9_2_10_1', 'EQF_9_2_10_2', 'EQF_9_2_11_1', 'EQF_9_2_12_1', 'EQR_9_2', 'EQR_9_2_1_1', 'EQR_9_2_1_2', 'EQR_9_2_2_2', 'EQR_9_2_2_5', 'EQR_9_2_6_1', 'EQR_9_2_6_2', 'EQR_9_2_10_1', 'EQR_9_2_10_2', 'EQR_9_2_11_1', 'EQR_9_2_12_1', 'TXAi_9_2', 'TXAi_9_2_1_1', 'TXAi_9_2_1_2', 'TXAi_9_2_2_2', 'TXAi_9_2_2_5', 'TXAi_9_2_6_1', 'TXAi_9_2_6_2', 'TXAi_9_2_10_1', 'TXAi_9_2_10_2', 'TXAi_9_2_11_1', 'TXAi_9_2_12_1', 'STGM_9_2', 'FROI_9_2', 'TLD_10_1', 'TL_10_1', 'TLPD_10_1', 'TLP_10_1', 'TBD_10_1', 'TB_10_1', 'LGR_10_1', 'LGR_APY_10_1', 'LW_10_1', 'LA_10_1', 'LFD_10_1', 'LFC_10_1', 'PFD_10_1', 'PFC_10_1', 'EQB_10_1', 'TXAs_10_1', 'EQF_10_1', 'EQF_10_1_1_1', 'EQF_10_1_1_2', 'EQF_10_1_2_2', 'EQF_10_1_2_5', 'EQF_10_1_6_1', 'EQF_10_1_6_2', 'EQF_10_1_9_1', 'EQF_10_1_9_2', 'EQF_10_1_11_1', 'EQF_10_1_12_1', 'EQR_10_1', 'EQR_10_1_1_1', 'EQR_10_1_1_2', 'EQR_10_1_2_2', 'EQR_10_1_2_5', 'EQR_10_1_6_1', 'EQR_10_1_6_2', 'EQR_10_1_9_1', 'EQR_10_1_9_2', 'EQR_10_1_11_1', 'EQR_10_1_12_1', 'TXAi_10_1', 'TXAi_10_1_1_1', 'TXAi_10_1_1_2', 'TXAi_10_1_2_2', 'TXAi_10_1_2_5', 'TXAi_10_1_6_1', 'TXAi_10_1_6_2', 'TXAi_10_1_9_1', 'TXAi_10_1_9_2', 'TXAi_10_1_11_1', 'TXAi_10_1_12_1', 'STGM_10_1', 'FROI_10_1', 'TLD_10_2', 'TL_10_2', 'TLPD_10_2', 'TLP_10_2', 'TBD_10_2', 'TB_10_2', 'LGR_10_2', 'LGR_APY_10_2', 'LW_10_2', 'LA_10_2', 'LFD_10_2', 'LFC_10_2', 'PFD_10_2', 'PFC_10_2', 'EQB_10_2', 'TXAs_10_2', 'EQF_10_2', 'EQF_10_2_1_1', 'EQF_10_2_1_2', 'EQF_10_2_2_2', 'EQF_10_2_2_5', 'EQF_10_2_6_1', 'EQF_10_2_6_2', 'EQF_10_2_9_1', 'EQF_10_2_9_2', 'EQF_10_2_11_1', 'EQF_10_2_12_1', 'EQR_10_2', 'EQR_10_2_1_1', 'EQR_10_2_1_2', 'EQR_10_2_2_2', 'EQR_10_2_2_5', 'EQR_10_2_6_1', 'EQR_10_2_6_2', 'EQR_10_2_9_1', 'EQR_10_2_9_2', 'EQR_10_2_11_1', 'EQR_10_2_12_1', 'TXAi_10_2', 'TXAi_10_2_1_1', 'TXAi_10_2_1_2', 'TXAi_10_2_2_2', 'TXAi_10_2_2_5', 'TXAi_10_2_6_1', 'TXAi_10_2_6_2', 'TXAi_10_2_9_1', 'TXAi_10_2_9_2', 'TXAi_10_2_11_1', 'TXAi_10_2_12_1', 'STGM_10_2', 'FROI_10_2', 'TLD_10_13', 'TL_10_13', 'TLPD_10_13', 'TLP_10_13', 'TBD_10_13', 'TB_10_13', 'LGR_10_13', 'LGR_APY_10_13', 'LW_10_13', 'LA_10_13', 'LFD_10_13', 'LFC_10_13', 'PFD_10_13', 'PFC_10_13', 'EQB_10_13', 'TXAs_10_13', 'EQF_10_13', 'EQF_10_13_1_13', 'EQF_10_13_11_13', 'EQR_10_13', 'EQR_10_13_1_13', 'EQR_10_13_11_13', 'TXAi_10_13', 'TXAi_10_13_1_13', 'TXAi_10_13_11_13', 'STGM_10_13', 'FROI_10_13', 'TLD_11_1', 'TL_11_1', 'TLPD_11_1', 'TLP_11_1', 'TBD_11_1', 'TB_11_1', 'LGR_11_1', 'LGR_APY_11_1', 'LW_11_1', 'LA_11_1', 'LFD_11_1', 'LFC_11_1', 'PFD_11_1', 'PFC_11_1', 'EQB_11_1', 'TXAs_11_1', 'EQF_11_1', 'EQF_11_1_1_1', 'EQF_11_1_1_2', 'EQF_11_1_2_2', 'EQF_11_1_2_5', 'EQF_11_1_6_1', 'EQF_11_1_6_2', 'EQF_11_1_9_1', 'EQF_11_1_9_2', 'EQF_11_1_10_1', 'EQF_11_1_10_2', 'EQF_11_1_12_1', 'EQR_11_1', 'EQR_11_1_1_1', 'EQR_11_1_1_2', 'EQR_11_1_2_2', 'EQR_11_1_2_5', 'EQR_11_1_6_1', 'EQR_11_1_6_2', 'EQR_11_1_9_1', 'EQR_11_1_9_2', 'EQR_11_1_10_1', 'EQR_11_1_10_2', 'EQR_11_1_12_1', 'TXAi_11_1', 'TXAi_11_1_1_1', 'TXAi_11_1_1_2', 'TXAi_11_1_2_2', 'TXAi_11_1_2_5', 'TXAi_11_1_6_1', 'TXAi_11_1_6_2', 'TXAi_11_1_9_1', 'TXAi_11_1_9_2', 'TXAi_11_1_10_1', 'TXAi_11_1_10_2', 'TXAi_11_1_12_1', 'STGM_11_1', 'FROI_11_1', 'TLD_11_13', 'TL_11_13', 'TLPD_11_13', 'TLP_11_13', 'TBD_11_13', 'TB_11_13', 'LGR_11_13', 'LGR_APY_11_13', 'LW_11_13', 'LA_11_13', 'LFD_11_13', 'LFC_11_13', 'PFD_11_13', 'PFC_11_13', 'EQB_11_13', 'TXAs_11_13', 'EQF_11_13', 'EQF_11_13_1_13', 'EQF_11_13_10_13', 'EQR_11_13', 'EQR_11_13_1_13', 'EQR_11_13_10_13', 'TXAi_11_13', 'TXAi_11_13_1_13', 'TXAi_11_13_10_13', 'STGM_11_13', 'FROI_11_13', 'TLD_12_1', 'TL_12_1', 'TLPD_12_1', 'TLP_12_1', 'TBD_12_1', 'TB_12_1', 'LGR_12_1', 'LGR_APY_12_1', 'LW_12_1', 'LA_12_1', 'LFD_12_1', 'LFC_12_1', 'PFD_12_1', 'PFC_12_1', 'EQB_12_1', 'TXAs_12_1', 'EQF_12_1', 'EQF_12_1_1_1', 'EQF_12_1_1_2', 'EQF_12_1_2_2', 'EQF_12_1_2_5', 'EQF_12_1_6_1', 'EQF_12_1_6_2', 'EQF_12_1_9_1', 'EQF_12_1_9_2', 'EQF_12_1_10_1', 'EQF_12_1_10_2', 'EQF_12_1_11_1', 'EQR_12_1', 'EQR_12_1_1_1', 'EQR_12_1_1_2', 'EQR_12_1_2_2', 'EQR_12_1_2_5', 'EQR_12_1_6_1', 'EQR_12_1_6_2', 'EQR_12_1_9_1', 'EQR_12_1_9_2', 'EQR_12_1_10_1', 'EQR_12_1_10_2', 'EQR_12_1_11_1', 'TXAi_12_1', 'TXAi_12_1_1_1', 'TXAi_12_1_1_2', 'TXAi_12_1_2_2', 'TXAi_12_1_2_5', 'TXAi_12_1_6_1', 'TXAi_12_1_6_2', 'TXAi_12_1_9_1', 'TXAi_12_1_9_2', 'TXAi_12_1_10_1', 'TXAi_12_1_10_2', 'TXAi_12_1_11_1', 'STGM_12_1', 'FROI_12_1'
    ]

pool_id_list = {
    "USDC": 1,
    "USDT": 2,
    "BUSD": 5,
    "SGETH": 13,
}

chain_id_list = {
    "ethereum": 1,
    "bsc": 2,
    "avalanche": 6,
    "polygon": 9,
    "arbitrum": 10,
    "optimism": 11,
    "fantom": 12,
}

event_topic_list = {
	'set': '0x545b620a3000f6303b158b321f06b4e95e28a27d70aecac8c6bdac4f48a9f6b3',
	'add': '0x1c482cb20f653d55406cc8aa89ebf482b8603c0ffebcf7e6182ff8ac1849d12d',
	'mint': '0xb4c03061fb5b7fed76389d5af8f2e0ddb09f8c70d1333abbb62582835e10accb',
	'burn': '0x49995e5dd6158cf69ad3e9777c46755a1a826a446c6416992167462dad033b2a',
	'swapremote': '0xfb2b592367452f1c437675bed47f5e1e6c25188c17d7ba01a12eb030bc41ccef',
	'swap': '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f',
	'deposit': '0x90890809c654f11d6e72a28fa60149770a0d11ec6c92319d6ceb2bb0a4ea1a15',
	'withdraw': '0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568',
}

tbl_schema = [
                {'name': 'block_no', 'type': 'int64'},
                {'name': 'timestamp', 'type': 'int64'},
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
                {'name': 'dst_chain', 'type': 'int64'},
                {'name': 'dst_pool', 'type': 'int64'}
                ]

chain_info_list = {
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
		'key': 'HU8VD7IF1INHGT8BR2XZH5UC1X58AISN1X',
		'url': 'https://api.ftmscan.com/api'
	}
}