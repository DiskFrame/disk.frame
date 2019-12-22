# disk.frame 0.3.2
* added (experimental) bloomfilter

# disk.frame 0.3.1
* urgent bug fix for group-by failing when the number chunks is 1; see Github #241

# disk.frame 0.3.0
* experimental one-stage group-by framework!
* bug fixes for data.table trigger by integration with tidyfast
* removed assertthat from imports
* add benchmarkme to Suggests


# disk.frame 0.2.1
* got rid of benchmarkme as a dependency
* added `hard_arrange` thanks to Jacky Poon
* added more .progress options for joins
* Using data.table::getDTthreads() as default number of workers
* multisession instead of multiprocess as default backend for data.table
* added support for R3.4
* fixed df_get_ram for R < 3.6

# disk.frame 0.2.0
* deprecated group_by, arrange, summarise
* add chunk_group_by, chunk_arrange, chunk_summarise
* fit GLMs with `dfglm`
* fixed so that dplyr function also work in mutate even with ~ in the name
* fixed disk.frame so that in works in functions too

# disk.frame 0.1.1

* Allowed `map` to accept multiple arguments. Thanks Knut Jägersberg for suggestion
* Fixed bug where if the CSV is larger than RAM then it fails by adding {LaF} backend
* Added {bigreadr} backend for reading large files by first splitting the file. This is the default behaviour
* Added {LaF} and {readr} chunk readers to `csv_to_disk.frame`
* fixed `write_disk.frame(...., shardby)` and other `shardby` functions including `rechunk` and `shard`
* added `df_ram_size()` to accurate determine RAM size for RStudio in R3.6
* added `show_ceremony` and `show_boiler_plate` to show setup code
