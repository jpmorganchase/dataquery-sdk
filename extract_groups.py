#!/usr/bin/env python3
"""Extract unique parent panel IDs from the panel configuration JSON."""

# All unique parent panel IDs from the provided JSON
parent_panels = {
    "BULL_BEAR_BUZZ", "CDS_Bond_Basis", "CDS_INDEX_TRANCHES", "CDXOptions",
    "CDX_EM_OPTIONS", "CR_IBOXX_TRSDATA", "DATA_ASSETS_ALPHA_GROUP",
    "DJ_CDX_NA_IHG", "DJ_CDX_NA_IHY", "EM_CDSwap", "EM_CDS_SOVEREIGN_NEW",
    "EM_CRV_SV", "EM_DI_CNTRCT", "EM_EASI", "FISWP_CMS_FWD",
    "FI_CS_EDSI", "FI_CS_FSI", "FI_FED_CUSTODY", "FI_GO_MSC_TRACE",
    "FI_IR_CMT", "FI_IR_FFR", "FI_MM_DTCC_FICC_SPN_ACT", "FI_MM_FI_CHLI",
    "FI_MM_FI_EULI", "FI_MM_FI_GBLI", "FI_MM_FI_OA", "FI_MM_FI_WI",
    "FI_MM_GC_EA", "FI_MM_GC_EM", "FI_MM_GC_JP", "FI_MM_GC_OE",
    "FI_MM_TSY_AGENCY_DISCOUNT_NOTES", "FI_MM_TSY_GENERAL_COLLATERAL_REPO",
    "FI_OPT_CMSCAP", "FI_OPT_FORW_MIDCUR_VOL", "FI_OPT_SWAP_MID_CRV_OPT",
    "FI_OPT_SWAP_OPT", "FI_OPT_YLD_CRV_SPRD", "FI_SOMA_WAM",
    "FI_SW_SV_AA", "FI_SW_SV_CE", "FI_SW_SV_EA", "FI_SW_SV_OA",
    "FI_TERM_PREMIUM", "FI_TSY_ISS", "FI_YIELD_ERRORS", "FU_POSTIONS_CFTC",
    "FXO_FP", "FXO_IVOL_INTRADAY", "FXO_SP", "FXO_V1",
    "FX_CASH_INTRADAY", "FX_EASIDX", "FX_ECN_VOLUMES", "FX_ECONOMIC",
    "FX_IMM", "FX_MEAN_HFFV", "FX_MEAN_IMM", "FX_MEAN_REER",
    "FX_SPOT_FWRD_AA", "FX_SPOT_FWRD_EA", "FX_SPOT_FWRD_OA",
    "FX_SW_IMM_FWD", "FX_SW_ROLLING_FWD", "FX_SYSM_STRG_CRY", "FX_VXY",
    "GENERALIST_SALES", "GFI_OPT_MID_CURVE_EUR_GBP", "GFI_OPT_YLD_CRV_SPRD",
    "GFI_OP_CF", "GFI_SWAPS_GLOBAL_CLOSES", "GFI_SWAPS_IMM_FWD",
    "HYGOptionsInternal", "IDX_EASI", "iTraxx_Europe", "iTraxx_Europe_Option_Atm",
    "iTraxx_Europe_Option_Delta", "iTraxx_Europe_Tranched",
    "ITRAXX_EUROPE_TRANCHE_RETURNS", "iTraxx_Europe_VTRAC_X",
    "iTraxx_INDICES_THEMATIC_INDICES", "JPMAQS", "MIST",
    "RESEARCH_EQUITY_ALL", "RESEARCH_FICC_ALL", "RESEARCH_FICC_CROSS_ASSETS",
    "RESEARCH_FICC_EM", "RESEARCH_FICC_MACRO", "RESEARCH_FICC_SPREAD",
    "RESEARCH_MEDIA_TRANSCRIPTS", "SALES_TRADERS", "SPECIALIST_SALES",
    "VI_SWAP"
}

# Sort for output
sorted_parents = sorted(parent_panels)
print(f"Total unique parent panel IDs: {len(sorted_parents)}\n")
print("GROUPS = [")
for parent in sorted_parents:
    print(f'    "{parent}",')
print("]")
