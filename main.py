import package.package as packages

import copy
from rpy2 import robjects as r
import sys


# packages: list[str]
# sample size: list[int]
# linkage fields: list[str]
# block: bool
# block_field: str
# number of cores: list[int]
# output file: list[str]

# Get the parameters from a file(1 arg) or from a user input(6 args) and clean the data for function calls
def main():
    if len(sys.argv) != 2 and len(sys.argv) != 8:
        print(len(sys.argv))
        raise ValueError("""Number of arguments must be 1 for a valid file name in the same directory
                          or 7 arguments in the form \"packages\", \"sample sizes\", \"linkage fields\",
                          \"block\", \"block_field\", \"output file\"""")

    params = []
    if len(sys.argv) == 2:
        with open(sys.argv[1], "r") as f:
            for i, line in enumerate(f.readlines()):
                params.append(line.strip())

    if len(sys.argv) == 8:
        for i in range(1, 8):
            params.append(sys.argv[i])

    params = clean(params)

    r.r.source("package/package.r")
    package = packages.Packages()

    for i, pack in enumerate(params[0]):
        params_copy = copy.deepcopy(params)
        
        if pack.lower() == "python_recordlinkage":
            package.python_recordlinkage(*params_copy[1:5], params_copy[5][i], params_copy[6][i])

        elif pack.lower() == "python_splink":
            package.python_splink(*params_copy[1:5], params_copy[6][i])

        elif pack.lower() == "r_recordlinkage":
            r.r['r_recordLinkage'](*params_copy[1:5], params_copy[6][i])

        elif pack.lower() == "r_fastlink":
            r.r['r_fastlink'](*params_copy[1:5], params_copy[5][i], params_copy[6][i])


def clean(lst: list) -> list:
    # Turn the string into a list
    # Packages to use for recordlinkage
    lst[0] = lst[0].strip('][').replace("\"", "").replace(" ", "").split(',')

    # Turn the string list into an int list
    # Sample sizes to test on (2,000-40,000 in 2,000 increments)
    lst[1] = lst[1].strip('][').replace("\"", "").replace(" ", "").split(',')
    temp_list = [int(k) for k in lst[1]]
    lst[1] = temp_list

    # Turn the string into a list
    # Linkage fields to test (id, first_name, middle_name, last_name, res_street_address, birth_year, zip_code)
    lst[2] = lst[2].strip('][').replace("\"", "").replace(" ", "").split(',')

    # Turn the string into a boolean
    # Whether blocking field will be used
    output = ""
    for i in lst[3]:
        if i.isalpha():
            output = "".join([output, i])

    if output.lower() == "true":
        lst[3] = True
    else:
        lst[3] = False

    # Strip the empty spaces
    # Name of the block field
    lst[4] = lst[4].replace(" ", "")

    # Turn the string into a list
    # Number of cores to be used
    lst[5] = lst[5].strip('][').replace("\"", "").replace(" ", "").split(',')
    temp_list = [int(k) for k in lst[5]]
    lst[5] = temp_list

    # Turn the string into a list
    # Names of the output files
    lst[6] = lst[6].strip('][').replace("\"", "").replace(" ", "").split(',')

    # Check if given parameters are valid
    package = ["python_recordlinkage", "python_splink", "r_recordlinkage", "r_fastlink"]
    link = ["first_name", "middle_name", "last_name", "res_street_address", "birth_year", "zip_code"]
    sample = [2000, 4000, 6000, 8000, 10000, 12000, 14000, 16000, 18000, 20000, 22000, 24000, 26000, 28000, 30000,
              32000, 34000, 36000, 38000, 40000]

    # If user gives unknown package
    if not all([name in package for name in lst[0]]):
        raise ValueError("Invalid package name")

    # If user gives unknown linkage field
    if not all([name in link for name in lst[2]]):
        raise ValueError("Invalid linkage field name")

    # If user gives unknown sample size
    if not all([size in sample for size in lst[1]]):
        raise ValueError("Invalid sample size")

    # If the given number of output file doesn't match the number of packages
    if len(lst[0]) != len(lst[6]):
        raise ValueError("Number of output files given must match the number of packages given")

    # If the given number of cores to use doesn't match the number of packages
    if len(lst[0]) != len(lst[5]):
        raise ValueError("Number of cores to use for packages must match the number of packages given")

    # If the given block field is already in the linkage field
    if lst[4] in lst[2]:
        raise ValueError("Block field cannot be in linkage field")

    return lst


if __name__ == "__main__":
    main()
