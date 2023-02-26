import packages_oop.package as packages
from rpy2 import robjects as r
import os

#runtime = [2000,4000,6000,8000,10000,12000,14000,16000,18000,20000,22000,24000,26000,28000,30000,32000,34000,36000,38000,40000]
runtime = 2000
linkage_field = ["first_name", "middle_name", "last_name", "res_street_address", "birth_year"]

r.r.source("packages_oop/package.r")
package = packages.Packages()

for i in range(1):
    #python_recordlinkage = package.python_recordlinkage(runtime, linkage_field, "zip_code", "results/python_recordlinkage.txt", 0)
    #splink = package.splink(runtime, linkage_field, "zip_code", "splink.txt",0)

    #r_recordlinkage = r.r['rRecordLinkage'](runtime, linkage_field, True, "zip_code", "id", "results/r_recordlinkage", 0)
    fastlink = r.r['fastlink_runtime'](runtime, linkage_field, True, "zip_code", "fastlink.txt", 0)