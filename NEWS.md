# disk.frame 0.1.1

* Allowed `map` to accept multiple arguments. Thanks Knut Jägersberg for suggestion
* Fixed bug where if the CSV is larger than RAM then it fails by adding {LaF} backend
* Added {bigreadr} backend for reading large files by first splitting the file. This is the default behaviour
* Added {LaF} and {readr} chunk readers to `csv_to_disk.frame`
* fixed `write_disk.frame(...., shardby)` and other `shardby` functions including `rechunk` and `shard`
* added `df_ram_size()` to accurate determine RAM size for RStudio in R3.6
* added `show_ceremony` and `show_boiler_plate` to show setup code