#!/bin/bash
#
# Simple deterministic RRFS_NA_3km JEDI retro on WCOSS2.
#
# This standalone configuration includes the WCOSS2 NA3km settings and the simple retro defaults.
# Link or copy it to ecf/defs/retro/config.sh and change the paths and server to your own.
#
# This is the ecflow equivalent of the Rocoto workflow's ush/config.sh, for the parts we control.
# It is sourced by ush/retro/setup_retro_links.sh, ecf/defs/retro/make_rrfs_retro_def.sh,
# ush/prod_clone/make_retro_def.sh and ush/retro/start_retro.sh, so edit values here instead of
# passing them on every command line. Most settings honor environment overrides, so one-off runs
# can still say e.g.
#
#   RETRO_START=20240510 ./make_rrfs_retro_def.sh
#
# What is NOT here, because the suite reads it at run time rather than at generation time:
#   fix/workflow/<WGF>/workflow.conf   job resources (NNODES_*, PPN_*, TPP_*) and science switches;
#                                      setup_ecf_links.sh copies the _prod or _dev version per
#                                      RESOURCE_CONFIG and applies the machine and retro overrides
#   versions/run.ver                   software versions used by the job cards
#
# ---------------------------------------------------------------------------------------------
# Machine
# ---------------------------------------------------------------------------------------------
# Sets how jobs are submitted and which machine-specific job settings setup_retro_links.sh applies.
# URSA or WCOSS2.
MACHINE=${MACHINE:-WCOSS2}
DOMAIN=${DOMAIN:-RRFS_NA_3km}

# ---------------------------------------------------------------------------------------------
# Where the workflow writes
# ---------------------------------------------------------------------------------------------
# Everything this run produces hangs off one base directory.
# A week of NA 3 km output is tens of TB, so the default is ptmp; note its purge policy.
# Give this experiment its own suite, work area, and archive directory.
EXPT_ID=${EXPT_ID:-det_na3km_3dvar_jedi_simple}
RRFS_SUITE=${RRFS_SUITE:-${EXPT_ID}}     # suite name in the generated definition
PRODCLONE_SUITE=${PRODCLONE_SUITE:-${RRFS_SUITE}_prod_clone}
RETRO_WORK_ROOT=${RETRO_WORK_ROOT:-/lfs/h2/emc/ptmp/${USER}/ecflow_rrfs}
RETRO_WORK_BASE=${RETRO_WORK_BASE:-${RETRO_WORK_ROOT}/${EXPT_ID}}

# ecflow job files and their output (the server creates the task directories underneath)
# Keep ecFlow checkpoints and logs on persistent noscrub storage.
export ECF_DATA_ROOT=${ECF_DATA_ROOT:-/lfs/h2/emc/da/noscrub/${USER}/ecflow}
export ECF_ROOT=${ECF_DATA_ROOT}
export ECF_HOME=${ECF_HOME:-${ECF_DATA_ROOT}/submit}
export ECF_OUTPUTDIR=${ECF_OUTPUTDIR:-${ECF_DATA_ROOT}/output}
export ECF_COMDIR=${ECF_COMDIR:-${ECF_HOME}}
export LFS_OUTPUTDIR=${LFS_OUTPUTDIR:-${ECF_HOME}}
OUTPUTDIR=${OUTPUTDIR:-${ECF_OUTPUTDIR}/${EXPT_ID}}
# COM output. envir-p1.h builds COMROOT as ${DEV_PTMP}/${USER}/ecflow_rrfs/para/com, so this is a
# base directory and the user and suite names are appended to it.
DEV_PTMP=${DEV_PTMP:-${RETRO_WORK_BASE}/ptmp}
# job working directories (DATAROOT). In stmp rather than under RETRO_WORK_BASE, which is in
# ptmp: a job's working directory is scratch for as long as that job runs, which is what stmp is
# for, and it keeps the COM output and the working directories on separate filesystems the way
# the Rocoto retros do (their STMP and PTMP).
DEV_DATAROOT=${DEV_DATAROOT:-/lfs/h2/emc/stmp/${USER}/ecflow_rrfs/${EXPT_ID}}

# ---------------------------------------------------------------------------------------------
# PBS
# ---------------------------------------------------------------------------------------------
PROJ=${PROJ:-RRFS}                        # the cards add -DEV: "-A RRFS-DEV"
QUEUE=${QUEUE:-dev}
QUEUE_ARCH=${QUEUE_ARCH:-dev_transfer} # HPSS transfers use the WCOSS2 transfer queue
# NCO keeps the production job sizes (115-node forecasts); EMC selects the smaller dev resources
# and 52-node forecast layouts that EMC's WCOSS2 parallels run.
RESOURCE_CONFIG=${RESOURCE_CONFIG:-EMC}

# ---------------------------------------------------------------------------------------------
# ecflow
# ---------------------------------------------------------------------------------------------
# Start the server with NCO's server_check.sh, run on (ssh to) an ecflow host of whichever machine
# is currently the development one: cdecflow01/02 for Cactus, ddecflow01/02 for Dogwood. After a
# production switch the other machine's hosts refuse the login. Its port is your uid + 2000.
# ECFLOW_VER must be a version "module avail ecflow" lists, since every job loads it.
ECFLOW_VER=${ECFLOW_VER:-5.6.0.14}
ECFLOW_HOST=${ECFLOW_HOST:-${ECF_HOST:-cdecflow01}}    # host running the server (head.h reads it as ECF_LOGHOST)
export ECF_HOST=${ECFLOW_HOST}

# ---------------------------------------------------------------------------------------------
# Input data
# ---------------------------------------------------------------------------------------------
# fix tree; setup_retro_links.sh links <repo>/fix to it. The production package's tree has the
# workflow.conf_dev and _prod files the retro needs; NCO may remove a version once the next is
# installed, so update the version here (or link a copy of your own) when the links break.
FIX_RRFS_DIR=${FIX_RRFS_DIR:-/lfs/h1/ops/prod/packages/rrfs.v1.0.28/fix}
# dev-sci's shared fix tree; the domain files (ecf/defs/retro/domains) link other grids' fix files
# from it
FIX_RRFS_SHARED=${FIX_RRFS_SHARED:-/lfs/h2/emc/lam/noscrub/emc.lam/FIX_RRFS}
# staged upstream data in NCO COM/DCOM layout (see make_links.sh in that directory)
RETRO_DATA_ROOT=${RETRO_DATA_ROOT:-/lfs/h2/emc/lam/noscrub/samuel.degelia/RRFS_RETRO_DATA_NCO}

# ---------------------------------------------------------------------------------------------
# Retro period and suite
# ---------------------------------------------------------------------------------------------
RETRO_START=${RETRO_START:-20240506}      # first retro day, and the day that cold starts
RETRO_END=20240506                       # last retro day
# YES relaxes the suite's 399 clock-time triggers so cycles are not gated on the wall clock
RETRO=${RETRO:-YES}

# Which workflow groups to run. FALSE gives that group's families defstatus complete, so they
# never run and nothing waiting on them blocks. Operations runs all of them.
RUN_ENKF=${RUN_ENKF:-FALSE}
RUN_ENSF=${RUN_ENSF:-FALSE}
RUN_FIREWX=${RUN_FIREWX:-FALSE}

# Two task types no retro needs; off as on Ursa. Both may work on WCOSS2, where operations runs them.
RUN_GEMPAK=${RUN_GEMPAK:-FALSE}
RUN_BUFRSND=${RUN_BUFRSND:-FALSE}

# Simple-workflow defaults.
RUN_SMOKE=${RUN_SMOKE:-TRUE}
DO_SPINUP=${DO_SPINUP:-FALSE}
DET_COLD_START_HRS="00 12"
GFS_FILE_FMT_ICS=grib2
EXTRN_MDL_ICS_OFFSET_HRS=6
FCST_LEN_HRS=${FCST_LEN_HRS:-3}
FCST_LEN_HRS_LONG=${FCST_LEN_HRS_LONG:-3}
RUN_NONVARCLD=${RUN_NONVARCLD:-FALSE}
RUN_UPDATE_BC=${RUN_UPDATE_BC:-FALSE}
RUN_GSIDIAG=${RUN_GSIDIAG:-TRUE}
RUN_LIGHTNING=${RUN_LIGHTNING:-FALSE}
DO_RADDA=${DO_RADDA:-FALSE}
RUN_RADAR=${RUN_RADAR:-FALSE}
RUN_POST=${RUN_POST:-TRUE}
RUN_SUBHOURLY_POST=${RUN_SUBHOURLY_POST:-FALSE}
DO_ARCHIVE_RETRO=${DO_ARCHIVE_RETRO:-TRUE}

# Use the RDHPCS account spelling for HPSS, independent of WCOSS2's lowercase $USER.
RDHPCS_USER=${RDHPCS_USER:-Donald.E.Lippi}
ARCHIVE_ROOT=${ARCHIVE_ROOT:-/NCEPDEV/emc-meso/5year/${RDHPCS_USER}/RRFSv1/May2024_retro/v1.2.0}
ARCHIVEDIR=${ARCHIVEDIR:-${ARCHIVE_ROOT}/${EXPT_ID}}

# The 84 h deterministic forecast at 00z, 06z, 12z and 18z, with its post, product generation and
# restarts. FALSE leaves the hourly 18 h forecasts alone and is the one setting that shortens a
# retro noticeably: the long forecast is 67 of the 81 minutes on each 6-hourly family's critical
# path, so a 13 km week goes from about 38 h to about 13 h. Operations needs it; a cycling or DA
# retro usually does not. Fire weather takes its initial and boundary conditions from the long
# forecast's post, so RUN_FIREWX must be FALSE as well, and the generator says so if it is not.
# The 85 boundary jobs at each long cycle are left alone, because the hourly cycles in the family
# share them; to cut those as well, shorten the forecast itself with FCST_LEN_HRS and
# FCST_LEN_HRS_LONG (see the simplified samples, config_*_simple.sh).
# JEDI analysis, the Rocoto config.sh "JEDI options". DA_SYSTEM_DET picks which det analysis
# runs, GSI (the operational route) or JEDI; the rest are read only when it is JEDI.
# setup_retro_links.sh writes them into det/workflow.conf and make_rrfs_retro_def.sh sizes the
# job from them.
DA_SYSTEM_DET=${DA_SYSTEM_DET:-JEDI}
# JCB templates in parm/jedi that the analysis renders its yaml from, conventional and
# reflectivity. The conventional one decides 3DVar or hybrid; this grid has only the
# 3DVar template so far.
JCB_CONFIG_CONV=${JCB_CONFIG_CONV:-rdas-atmosphere-templates-fv3_na3km.yaml}
JCB_CONFIG_DBZ=${JCB_CONFIG_DBZ-}
# Whether the analysis also runs on cold-start cycles, and whether synthetic radiosonde
# temperature and humidity levels are added the way GSI's sonde_ext does
DO_DACOLD=${DO_DACOLD:-FALSE}
EXT_SONDE=TRUE
# Job size. Nodes x tasks per node must match the template's layout,
# the 40x40 layout in the na3km template: 1600 tasks
NNODES_ANALYSIS_JEDI=${NNODES_ANALYSIS_JEDI:-16}
PPN_ANALYSIS_JEDI=${PPN_ANALYSIS_JEDI:-100}
TPP_ANALYSIS_JEDI=${TPP_ANALYSIS_JEDI:-1}
WALLTIME_ANALYSIS_JEDI=${WALLTIME_ANALYSIS_JEDI:-00:50:00}
# JEDI EnKF. DA_SYSTEM_ENKF picks which EnKF analysis runs, GSI or JEDI (GETKF), independent of
# DA_SYSTEM_DET; the rest are read only when it is JEDI. setup_retro_links.sh writes them into
# enkf/workflow.conf.
DA_SYSTEM_ENKF=${DA_SYSTEM_ENKF:-GSI}
JCB_CONFIG_ENKF=${JCB_CONFIG_ENKF:-rdas-atmosphere-templates-fv3_na3km_getkf.yaml}
NNODES_ANALYSIS_ENKF_JEDI=${NNODES_ANALYSIS_ENKF_JEDI:-48}
PPN_ANALYSIS_ENKF_JEDI=${PPN_ANALYSIS_ENKF_JEDI:-40}
TPP_ANALYSIS_ENKF_JEDI=${TPP_ANALYSIS_ENKF_JEDI:-1}
WALLTIME_ANALYSIS_ENKF_JEDI=${WALLTIME_ANALYSIS_ENKF_JEDI:-01:00:00}

DO_LONG_FORECAST=${DO_LONG_FORECAST:-TRUE}

# YES keeps the working directories under stmp after jobs finish (sets KEEPDATA and KEEP_TMP)
KEEPDATA=${KEEPDATA:-YES}

# suite definition to copy, relative to ecf/defs
BASE_DEF=${BASE_DEF:-nco_para/rrfs_nco_para.def}
RRFS_VER=${RRFS_VER:-v1.0}
ENVIR=${ENVIR:-prod}
MACHINE_SITE=${MACHINE_SITE:-development}

# Keep the GSI observation mode consistent with the radar setting unless explicitly overridden.
if [ -z "${OB_TYPE:-}" ]; then
  if [ "${RUN_RADAR}" = "TRUE" ]; then
    OB_TYPE=conv_dbz
  else
    OB_TYPE=conv
  fi
fi
