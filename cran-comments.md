## Resubmission 6
* Removed commented out code in examples
* removed \dontrun{} from examples
* No functions write by default to user's home filespace
* Remove `options` so that no suer's options are changed

## Test environments
* local Windows 10 Pro install, R 3.6.1
* local Windows 10 Pro install, R devel (as of 2019-08-03)
* local Linux/Ubuntu install, R 3.6.1
* local Linux/Ubuntu install, R devel (as of 2019-08-03)

## R CMD check results
There were no ERRORs nor WARNINGs nor NOTEs when run locally

## Downstream dependencies
There are currently no downstream dependencies for this package as it is a new package

## Resubmission 5
* reduced title to less than 65 chars as requested
* no longer uses T/F anywhere and uses TRUE/FALSE
* no longer prints or cats to standard output and instead uses message()
* updated example so that nothing writes to home filespace including inside \dontrun
* using no more than two cores in all functions including setup_disk.frame
* updated example so that interactive 

## Resubmission 4
* added more examples; almost one example per function
* also updated examples and vignettes to not write to user's home filespace

## Resubmission 3
Fixed the note
- non-canonical URL to CRAN package which I had missed in resubmission 2

## Resubmission 2
Fixed note
* CRAN URL not in canonical form

## Resubmission
previously fixed notes 
* Non-FOSS package license (file LICENSE): now using MIT + file LICENSE in correct format
* Package has a VignetteBuilder field but no prebuilt vignette index: also added Vignette
* Also removed invalid URLs from README

