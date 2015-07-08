from distutils.core import setup
import py2exe


class Target:
    def __init__(self, **kw):
        self.__dict__.update(kw)
        # for the versioninfo resources
        self.version = "1.0.1"
        self.company_name = "Mozilla"
        self.copyright = "Mozilla Public License 2.0"
        self.name = "Metric Collective"


metcollect = Target(
    # used for the versioninfo resource
    description = "Collect system metrics and send to Graphite",
    # what to build.  For a service, the module name (not the
    # filename) must be specified!
    modules = ["metcollect"],
    cmdline_style='pywin32',
    )

setup(
    options = {"py2exe": {"compressed": 1, "bundle_files": 1, "dll_excludes": ['msvcr71.dll', "IPHLPAPI.DLL", "NSI.dll",  "WINNSI.DLL",  "WTSAPI32.dll"]} },
    console=["metcollect.py"],
    zipfile = None,
    
    service=[metcollect]
)