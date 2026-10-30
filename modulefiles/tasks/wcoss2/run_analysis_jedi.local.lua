help([[
Load the RDASApp WCOSS2 environment for JEDI analysis tasks.
]])

local rrfs_home = os.getenv("HOMErrfs")
if not rrfs_home then
  LmodError("HOMErrfs must be set before loading run_analysis_jedi.local")
end

-- Use the same RDASApp stack Python and libraries used by the application build.
prepend_path("MODULEPATH", pathJoin(rrfs_home, "sorc/RDASApp/modulefiles"))
load("RDAS/wcoss2.intel")
