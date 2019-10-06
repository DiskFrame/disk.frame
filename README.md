
<!-- README.md is generated from README.Rmd. Please edit that file -->

# disk.frame <img src="inst/figures/disk.frame.png" align="right">

<details>

<summary>Please take a moment to star the disk.frame Github repo if you
like disk.frame. It keeps me going.</summary>
<iframe src="https://ghbtns.com/github-btn.html?user=xiaodaigh&repo=disk.frame&type=star&count=true&size=large" frameborder="0" scrolling="0" width="160px" height="30px"></iframe>

</details>

<!-- badges: start -->

<!-- ![disk.frame logo](inst/figures/disk.frame.png?raw=true "disk.frame logo") -->

[![](https://cranlogs.r-pkg.org/badges/disk.frame)](https://cran.r-project.org/package=disk.frame)
[![](http://cranlogs.r-pkg.org/badges/grand-total/disk.frame)](https://cran.r-project.org/package=disk.frame)
[![Sponsors on Open
Collective](https://opencollective.com/diskframe/sponsors/badge.svg)](#sponsors)
[![Backers on Open
Collective](https://opencollective.com/diskframe/backers/badge.svg)](#backers)
[![Travis build
status](https://travis-ci.org/xiaodaigh/disk.frame.svg?branch=master)](https://travis-ci.org/xiaodaigh/disk.frame)
[![AppVeyor build
status](https://ci.appveyor.com/api/projects/status/github/xiaodaigh/disk.frame?branch=master&svg=true)](https://ci.appveyor.com/project/xiaodaigh/disk.frame)
<!-- badges: end -->

# Introduction

How can I manipulate structured tabular data that doesn’t fit into
Random Access Memory (RAM)? Use `{disk.frame}`\!

In a nutshell, `{disk.frame}` makes use of two simple ideas

1)  split up a larger-than-RAM dataset into chunks and store each chunk
    in a separate file inside a folder and
2)  provide a convenient API to manipulate these chunks

`{disk.frame}` performs a similar role to distributed systems such as
Apache Spark, Python’s Dask, and Julia’s JuliaDB.jl for *medium data*
which are datasets that are too large for RAM but not quite large enough
to qualify as *big data* that require distributing processing over many
computers to be effective.

## Sponsors

<a href="https://opencollective.com/diskframe#sponsors" target="_blank"><img src="https://opencollective.com/diskframe/sponsors.svg?width=890"></a>

## Installation

You can install the released version of `{disk.frame}` from
[CRAN](https://CRAN.R-project.org) with:

``` r
install.packages("disk.frame")
```

And the development version from [GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("xiaodaigh/disk.frame")
```

On some platforms, such as SageMaker, you may need to explicitly specify
a repo like this

``` r
install.packages("disk.frame", repo="https://cran.rstudio.com")
```

## Vignettes and articles

Please see these vignettes and articles about `{disk.frame}`

  - [Quick start:
    `{disk.frame}`](http://daizj.net/disk.frame/articles/intro-disk-frame.html)
    which replicates the `sparklyr` vignette for manipulating the
    `nycflights13` flights data.
  - [Ingesting data into
    `{disk.frame}`](http://diskframe.com/articles/ingesting-data.html)
    which lists some commons way of creating disk.frames
  - [`{disk.frame}` can be more
    epic\!](http://diskframe.com/articles/more-epic.html) shows some
    ways of loading large CSVs and the importance of `srckeep`
  - [Fitting GLMs (including logistic
    regression)](http://diskframe.com/articles/glm.html) introduces the
    `dfglm` function for fitting generalized linear models
  - [Using data.table syntax with
    disk.frame](http://diskframe.com/articles/data-table-syntax.html)
  - [disk.frame concepts](http://diskframe.com/articles/concepts.html)
  - [Benchmark 1: disk.frame vs Dask vs
    JuliaDB](http://diskframe.com/articles/vs-dask-juliadb.html)

### Interested in learning `{disk.frame}` in a structured course?

Please register your interest at:

<https://leanpub.com/c/taminglarger-than-ramwithdiskframe>

## Common questions

### a) What is `{disk.frame}` and why create it?

`{disk.frame}` is an R package that provides a framework for
manipulating larger-than-RAM structured tabular data on disk
efficiently. The reason one would want to manipulate data on disk is
that it allows arbitrarily large datasets to be processed by R. In other
words, we go from “R can only deal with data that fits in RAM” to “R can
deal with any data that fits on disk”. See the next section.

### b) How is it different to `data.frame` and `data.table`?

A `data.frame` in R is an in-memory data structure, which means that R
must load the data in its entirety into RAM. A corollary of this is that
only data that can fit into RAM can be processed using `data.frame`s.
This places significant restrictions on what R can process with minimal
hassle.

In contrast, `{disk.frame}` provides a framework to store and manipulate
data on the hard drive. It does this by loading only a small part of the
data, called a chunk, into RAM; process the chunk, write out the results
and repeat with the next chunk. This chunking strategy is widely applied
in other packages to enable processing large amounts of data in R, for
example, see [`chunkded`](https://github.com/edwindj/chunked)
[`arkdb`](https://github.com/ropensci/arkdb), and
[`iotools`](https://github.com/s-u/iotools).

Furthermore, there is a row-limit of 2^31 for `data.frame`s in R; hence
an alternate approach is needed to apply R to these large datasets. The
chunking mechanism in `{disk.frame}` provides such an avenue to enable
data manipulation beyond the 2^31 row limit.

### c) How is `{disk.frame}` different to previous “big” data solutions for R?

R has many packages that can deal with larger-than-RAM datasets,
including `ff` and `bigmemory`. However, `ff` and `bigmemory` restrict
the user to primitive data types such as double, which means they do not
support character (string) and factor types. In contrast, `{disk.frame}`
makes use of `data.table::data.table` and `data.frame` directly, so all
data types are supported. Also, `{disk.frame}` strives to provide an API
that is as similar to `data.frame`’s where possible. `{disk.frame}`
supports many `dplyr` verbs for manipulating `disk.frame`s.

Additionally, `{disk.frame}` supports parallel data operations using
infrastructures provided by the excellent [`future`
package](https://CRAN.R-project.org/package=future) to take advantage of
multi-core CPUs. Further, `{disk.frame}` uses state-of-the-art data
storage techniques such as fast data compression, and random access to
rows and columns provided by the [`fst`
package](http://www.fstpackage.org/) to provide superior data
manipulation speeds.

### d) How does `{disk.frame}` work?

`{disk.frame}` works by breaking large datasets into smaller individual
chunks and storing the chunks in `fst` files inside a folder. Each chunk
is a `fst` file containing a `data.frame/data.table`. One can construct
the original large dataset by loading all the chunks into RAM and
row-bind all the chunks into one large `data.frame`. Of course, in
practice this isn’t always possible; hence why we store them as smaller
individual chunks.

`{disk.frame}` makes it easy to manipulate the underlying chunks by
implementing `dplyr` functions/verbs and other convenient functions
(e.g. the `map.disk.frame(a.disk.frame, fn, lazy = F)` function which
applies the function `fn` to each chunk of `a.disk.frame` in parallel).
So that `{disk.frame}` can be manipulated in a similar fashion to
in-memory `data.frame`s.

### e) How is `{disk.frame}` different from Spark, Dask, and JuliaDB.jl?

Spark is primarily a distributed system that also works on a single
machine. Dask is a Python package that is most similar to
`{disk.frame}`, and JuliaDB.jl is a Julia package. All three can
distribute work over a cluster of computers. However, `{disk.frame}`
currently cannot distribute data processes over many computers, and is,
therefore, single machine focused.

In R, one can access Spark via `sparklyr`, but that requires a Spark
cluster to be set up. On the other hand `{disk.frame}` requires
zero-setup apart from running `install.packages("disk.frame")` or
`devtools::install_github("xiaodaigh/disk.frame")`.

Finally, Spark can only apply functions that are implemented for Spark,
whereas `{disk.frame}` can use any function in R including user-defined
functions.

### f) How is `{disk.frame}` different from multidplyr, partools and distributedR?

The packages [multidplyr](https://github.com/tidyverse/multidplyr)
doesn’t seem to be disk-focused and hence does not allow arbitrarily
large dataset to be manipulated; the focus on parallel processing is
similar to disk.frame though. For partools
\[<https://matloff.wordpress.com/2015/08/05/partools-a-sensible-r-package-for-large-data-sets/>\],
it seems to use it’s own verbs for aggregating data instead of relying
on existing verbs provided by data.table and dplyr. The package
[`distributedR`](https://github.com/vertica/DistributedR) hasn’t been
updated for a few years and also seems to require its own functions and
verbs.

## Backers

I would like to thank our backer:

<a href="https://opencollective.com/diskframe#backers" target="_blank"><img src="https://opencollective.com/diskframe/backers.svg?width=890"></a>

# Example usage

## Set-up `{disk.frame}`

`{disk.frame}` works best if it can process multiple data chunks in
parallel. The best way to set-up `{disk.frame}` so that each CPU core
runs a background worker is by using

``` r
setup_disk.frame()

# this allows large datasets to be transferred between sessions
options(future.globals.maxSize = Inf)
```

The `setup_disk.frame()` sets up background workers equal to the number
of CPU cores; please note that, by default, hyper-threaded cores are
counted as one not two.

Alternatively, one may specify the number of workers using
`setup_disk.frame(workers = n)`.

## Quick-start

``` r
suppressPackageStartupMessages(library(disk.frame))
library(nycflights13)

# this will setup disk.frame's parallel backend with number of workers equal to the number of CPU cores (hyper-threaded cores are counted as one not two)
setup_disk.frame()
#> The number of workers available for disk.frame is 6
# this allows large datasets to be transferred between sessions
options(future.globals.maxSize = Inf)

# convert the flights data.frame to a disk.frame
# optionally, you may specify an outdir, otherwise, the 
flights.df <- as.disk.frame(nycflights13::flights)
```

To find out where the disk.frame is stored on disk:

``` r
# where is the disk.frame stored
attr(flights.df, "path")
#> [1] "C:\\Users\\RTX2080\\AppData\\Local\\Temp\\Rtmp2JaPvM\\file1878c9d20d2.df"
```

A number of data.frame functions are implemented for disk.frame

``` r
# get first few rows
head(flights.df)
#>    year month day dep_time sched_dep_time dep_delay arr_time
#> 1: 2013     1   1      517            515         2      830
#> 2: 2013     1   1      533            529         4      850
#> 3: 2013     1   1      542            540         2      923
#> 4: 2013     1   1      544            545        -1     1004
#> 5: 2013     1   1      554            600        -6      812
#> 6: 2013     1   1      554            558        -4      740
#>    sched_arr_time arr_delay carrier flight tailnum origin dest air_time
#> 1:            819        11      UA   1545  N14228    EWR  IAH      227
#> 2:            830        20      UA   1714  N24211    LGA  IAH      227
#> 3:            850        33      AA   1141  N619AA    JFK  MIA      160
#> 4:           1022       -18      B6    725  N804JB    JFK  BQN      183
#> 5:            837       -25      DL    461  N668DN    LGA  ATL      116
#> 6:            728        12      UA   1696  N39463    EWR  ORD      150
#>    distance hour minute           time_hour
#> 1:     1400    5     15 2013-01-01 05:00:00
#> 2:     1416    5     29 2013-01-01 05:00:00
#> 3:     1089    5     40 2013-01-01 05:00:00
#> 4:     1576    5     45 2013-01-01 05:00:00
#> 5:      762    6      0 2013-01-01 06:00:00
#> 6:      719    5     58 2013-01-01 05:00:00
```

``` r
# get last few rows
tail(flights.df)
#>    year month day dep_time sched_dep_time dep_delay arr_time
#> 1: 2013     9  30       NA           1842        NA       NA
#> 2: 2013     9  30       NA           1455        NA       NA
#> 3: 2013     9  30       NA           2200        NA       NA
#> 4: 2013     9  30       NA           1210        NA       NA
#> 5: 2013     9  30       NA           1159        NA       NA
#> 6: 2013     9  30       NA            840        NA       NA
#>    sched_arr_time arr_delay carrier flight tailnum origin dest air_time
#> 1:           2019        NA      EV   5274  N740EV    LGA  BNA       NA
#> 2:           1634        NA      9E   3393    <NA>    JFK  DCA       NA
#> 3:           2312        NA      9E   3525    <NA>    LGA  SYR       NA
#> 4:           1330        NA      MQ   3461  N535MQ    LGA  BNA       NA
#> 5:           1344        NA      MQ   3572  N511MQ    LGA  CLE       NA
#> 6:           1020        NA      MQ   3531  N839MQ    LGA  RDU       NA
#>    distance hour minute           time_hour
#> 1:      764   18     42 2013-09-30 18:00:00
#> 2:      213   14     55 2013-09-30 14:00:00
#> 3:      198   22      0 2013-09-30 22:00:00
#> 4:      764   12     10 2013-09-30 12:00:00
#> 5:      419   11     59 2013-09-30 11:00:00
#> 6:      431    8     40 2013-09-30 08:00:00
```

``` r
# number of rows
nrow(flights.df)
#> [1] 336776
```

``` r
# number of columns
ncol(flights.df)
#> [1] 19
```

## Example: dplyr verbs

### Group by

Group-by in disk.frame are performed within each chunk, hence a
two-stage group by is required to obtain the correct group by results.
The two-stage approach is preferred for performance reasons too.

To perform group-by one needs to do it in two-stage approach as the
group-by’s are performed within each chunk. This will be addressed in a
future package called `disk.frame.db`, but for now two-stage aggregation
is the best to do group-bys in `{disk.frame}`.

``` r
flights.df = as.disk.frame(nycflights13::flights)

flights.df %>%
  srckeep(c("year","distance")) %>%  # keep only carrier and distance columns
  chunk_group_by(year) %>% 
  chunk_summarise(sum_dist = sum(distance)) %>% # this does a count per chunk
  collect
#> # A tibble: 6 x 2
#>    year sum_dist
#>   <int>    <dbl>
#> 1  2013 57446059
#> 2  2013 59302212
#> 3  2013 56585094
#> 4  2013 58476357
#> 5  2013 59407019
#> 6  2013 59000866
```

This is two-stage group-by in action

``` r
# need a 2nd stage to finalise summing
flights.df %>%
  srckeep(c("year","distance")) %>%  # keep only carrier and distance columns
  chunk_group_by(year) %>% 
  chunk_summarise(sum_dist = sum(distance)) %>% # this does a count per chunk
  collect %>% 
  group_by(year) %>% 
  summarise(sum_dist = sum(sum_dist))
#> # A tibble: 1 x 2
#>    year  sum_dist
#>   <int>     <dbl>
#> 1  2013 350217607
```

Here an example of using `filter`

``` r
# filter
pt = proc.time()
df_filtered <-
  flights.df %>% 
  filter(month == 1)
cat("filtering a < 0.1 took: ", data.table::timetaken(pt), "\n")
#> filtering a < 0.1 took:  0.010s elapsed (0.010s cpu)
nrow(df_filtered)
#> [1] 336776
```

You can mix group-by with other dplyr verbs as below

``` r
pt = proc.time()
res1 <- flights.df %>% 
  srckeep(c("month", "dep_delay")) %>% 
  filter(month <= 6) %>% 
  mutate(qtr = ifelse(month <= 3, "Q1", "Q2")) %>% 
  chunk_group_by(qtr) %>% 
  chunk_summarise(sum_delay = sum(dep_delay, na.rm = TRUE), n = n()) %>% 
  collect %>%
  group_by(qtr) %>% 
  summarise(sum_delay = sum(sum_delay), n = sum(n)) %>% 
  mutate(avg_delay = sum_delay/n)
cat("group by took: ", data.table::timetaken(pt), "\n")
#> group by took:  0.620s elapsed (0.160s cpu)

collect(res1)
#> # A tibble: 2 x 4
#>   qtr   sum_delay     n avg_delay
#>   <chr>     <dbl> <int>     <dbl>
#> 1 Q1       892053 80789      11.0
#> 2 Q2      1319941 85369      15.5
```

However, a one-stage `group_by` is possible with a `hard_group_by` to
first rechunk the disk.frame. This **not** recommended for performance
reasons, as it can quite slow.

``` r
pt = proc.time()
res1 <- flights.df %>% 
  srckeep(c("month", "dep_delay")) %>% 
  filter(month <= 6) %>% 
  mutate(qtr = ifelse(month <= 3, "Q1", "Q2")) %>% 
  hard_group_by(qtr) %>% # hard group_by is MUCH SLOWER but avoid a 2nd stage aggregation
  chunk_summarise(avg_delay = mean(dep_delay, na.rm = TRUE)) %>% 
  collect
#> Appending disk.frames:
cat("group by took: ", data.table::timetaken(pt), "\n")
#> group by took:  1.160s elapsed (0.240s cpu)

collect(res1)
#> # A tibble: 2 x 2
#>   qtr   avg_delay
#>   <chr>     <dbl>
#> 1 Q1         11.4
#> 2 Q2         15.9
```

## Example: data.table syntax

``` r
library(data.table)
#> 
#> Attaching package: 'data.table'
#> The following object is masked from 'package:purrr':
#> 
#>     transpose
#> The following objects are masked from 'package:dplyr':
#> 
#>     between, first, last

grp_by_stage1 = 
  flights.df[
    keep = c("month", "distance"), # this analysis only required "month" and "dist" so only load those
    month <= 6, 
    .(sum_dist = sum(distance)), 
    .(qtr = ifelse(month <= 3, "Q1", "Q2"))
    ]

grp_by_stage1
#>    qtr sum_dist
#> 1:  Q1 27188805
#> 2:  Q1   953578
#> 3:  Q1 53201567
#> 4:  Q2  3383527
#> 5:  Q2 58476357
#> 6:  Q2 27397926
```

The result `grp_by_stage1` is a `data.table` so we can finish off the
two-stage aggregation using data.table syntax

``` r
grp_by_stage2 = grp_by_stage1[,.(sum_dist = sum(sum_dist)), qtr]

grp_by_stage2
#>    qtr sum_dist
#> 1:  Q1 81343950
#> 2:  Q2 89257810
```

## Hex logo

![disk.frame logo](inst/figures/logo.png?raw=true)

## Contributors

This project exists thanks to all the people who contribute.
<a href="https://github.com/xiaodaigh/disk.frame/graphs/contributors"><img src="https://opencollective.com/diskframe/contributors.svg?width=890&button=false" /></a>

## Current Priorities

The work priorities at this stage are

1.  Bugs
2.  Urgent feature implementations that can improve an awful
    user-experience
3.  More vignettes covering every aspect of disk.frame
4.  Comprehensive Tests
5.  Comprehensive Documentation
6.  More features

## Blogs and other resources

| Title                                                                                                                                 | Author          | Date     | Description                                                                                        |
| ------------------------------------------------------------------------------------------------------------------------------------- | --------------- | -------- | -------------------------------------------------------------------------------------------------- |
| [`{disk.frame}` is epic](https://www.brodrigues.co/blog/2019-09-03-disk_frame/)                                                       | Bruno Rodriguez | 20190903 | It’s about loading a 30G file into `{disk.frame}`                                                  |
| [My top 10 R packages for data analytics](https://www.actuaries.digital/2019/09/26/my-top-10-r-packages-for-data-analytics/)          | Jacky Poon      | 20190903 | `{disk.frame}` was number 3                                                                        |
| [useR\! 2019 presentation video](https://www.youtube.com/watch?v=3XMTyi_H4q4)                                                         | Dai ZJ          | 20190803 |                                                                                                    |
| [useR\! 2019 presentation slides](https://www.beautiful.ai/player/-LphQ0YaJwRektb8nZoY)                                               | Dai ZJ          | 20190803 |                                                                                                    |
| [Split-apply-combine for Maximum Likelihood Estimation of a linear model](https://www.brodrigues.co/blog/2019-10-05-parallel_maxlik/) | Bruno Rodriguez | 20191006 | `{disk.frame}` used in helping to create a maximum likelihood estimation program for linear models |
| [Emma goes to useR\! 2019](https://emmavestesson.netlify.com/2019/07/user2019/)                                                       | Emma Vestesson  | 20190716 | The first mention of `{disk.frame}` in a blog post                                                 |

## Open Collective

If you like `{disk.frame}` and want to speed up its development or
perhaps you have a feature request? Please consider sponsoring
`{disk.frame}` on Open Collective

### Backers

Thank you to all our backers\! \[[Become a
backer](https://opencollective.com/diskframe#backer)\]

<a href="https://opencollective.com/diskframe#backers" target="_blank"><img src="https://opencollective.com/diskframe/backers.svg?width=890"></a>

### Sponsors

Support `{disk.frame}` development by becoming a sponsor. Your logo will
show up here with a link to your website. \[[Become a
sponsor](https://opencollective.com/diskframe#sponsor)\]

<a href="https://opencollective.com/diskframe#sponsors" target="_blank"><img src="https://opencollective.com/diskframe/sponsors.svg?width=890"></a>

## Contact me for consulting

**Do you need help with machine learning and data science in R, Python,
or Julia?** I am available for Machine Learning/Data
Science/R/Python/Julia consulting\! [Email
me](mailto:dzj@analytixware.com)
