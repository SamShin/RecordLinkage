import packages_oop.package as packages
from rpy2 import robjects as r
import sys

# runtime = [2000,4000,6000,8000,10000,12000,14000,16000,18000,20000,22000,24000,26000,28000,30000,32000,34000,36000,38000,40000]

if len(sys.argv) != 2 or len(sys.argv) != 7:
    raise ValueError("""Number of arguments must be 1 for a valid file name in the same directory
                      or 5 arguments in the form \"packages\", \"sample sizes\", \"linkage fields\",
                      \"block\", \"block_field\", \"output file\"""")


# packages: str
# sample size: list[int]
# linkage fields: list[str]
# block: bool
# block_field: list[str]
# output file: str

# Get the parameters from a file(1 arg) or from a user input(6 args) and clean the data for function calls
def main():
    params = []
    if len(sys.argv) == 2:
        with open(sys.argv[1], "r") as f:
            for i, line in enumerate(f.readlines()):
                params.append(line.strip())

    if len(sys.argv) == 7:
        for i in range(1, 7):
            params.append(sys.argv[i])

    params = clean(params)

    package = packages.Packages()
    for pack in params[0]:
        if pack.lower() == "python_recordlinkage":
            package.python_recordlinkage(*params)

        elif pack.lower() == "python_splink":
            package.python_splink(*params)

        elif pack.lower() == "r_recordlinkage":
            r.r['r_recordLinkage'](*params)

        elif pack.lower() == "r_fastlink":
            r.r['fastlink_runtime'](*params)

    # runtime = [2000]
    # linkage_field = ["first_name", "middle_name", "last_name", "res_street_address", "birth_year"]

    # #r.r.source("packages_oop/package.r")
    # package = packages.Packages()

    # for i in range(2):
    #     python_recordlinkage = package.python_recordlinkage(runtime, linkage_field, "zip_code", "results/python_recordlinkage.txt", 0)
    #     splink = package.splink(runtime, linkage_field, "zip_code", "results/splink.txt",0)

    #     #r_recordlinkage = r.r['rRecordLinkage'](runtime, linkage_field, True, "zip_code", "id", "results/r_recordlinkage", 0)
    #     #fastlink = r.r['fastlink_runtime'](runtime, linkage_field, True, "zip_code", "results/fastlink.txt", 0)


def clean(lst: list) -> list:

    # Turn the string into a list
    lst[0] = lst[0].strip('][').split(',')

    # Turn the string list into an int list
    lst[1] = lst[1].strip('][').split(',')
    temp_list = [int(k) for k in lst[1]]
    lst[1] = temp_list

    # Turn the string into a list
    lst[2] = lst[2].strip('][').split(',')

    # Turn the string into a boolean
    output = ""
    for i in lst[3]:
        if i.isalpha():
            output = "".join([output, i])

    if output.lower() == "true":
        lst[3] = True
    else:
        lst[3] = False

    return lst


if __name__ == "__main__":
    main()
