## Re- Submission for v0.3.4
* Updated and removed vignette
* I keep getting this error on R-devel, but when I run the code in the REPL it's fine, so I suspect it's a bug with R-devel.
```
Error in print.default(toprint, right = TRUE, quote = quote) : 
    invalid 'useSource' argument
  Calls: <Anonymous> -> print.data.table -> print -> print.default
  Execution halted
```

## Test environments
* local Windows 10 Pro install, R 3.6.2
* local Windows 10 Pro install, R devel (as of 2020-02-19)
* local Linux/Ubuntu install, R 3.6.2
* local Linux/Ubuntu install, R devel (as of 2020-02-19)

## R CMD check results
There were no ERRORs nor WARNINGs nor NOTEs when run locally

## Downstream dependencies
I have tested the relevant tests in {drake}, the only downstream dependency
