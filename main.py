import packages_oop.package as packages
from rpy2 import robjects as r

runtime = [2000]
linkage_field = ["first_name", "middle_name", "last_name", "res_street_address", "birth_year"]

r.r.source("packages_oop/package.r")
package = packages.Packages()

for i in range(2):
    # package.python_recordlinkage(runtime, linkage_field, False, "zip_code", "results/python_recordlinkage.txt")
    # package.python_splink(runtime, linkage_field, False, "zip_code", "results/splink.txt")

    # r.r['r_recordlinkage'](runtime, linkage_field, True, "zip_code", "id", "results/r_recordlinkage")
    r.r['r_fastlink'](runtime, linkage_field, True, "zip_code", "fastlink.txt")
