## Submission for v0.3.3
* Removed usage bloom-filter which was causing issues with solaris 

## Test environments
* local Windows 10 Pro install, R 3.6.2
* local Windows 10 Pro install, R devel (as of 2019-12-17)
* local Linux/Ubuntu install, R 3.6.2
* local Linux/Ubuntu install, R devel (as of 2019-12-17)

## R CMD check results
There were no ERRORs nor WARNINGs nor NOTEs when run locally

## Downstream dependencies
I have tested the relevant tests in {drake}, the only downstream dependency
