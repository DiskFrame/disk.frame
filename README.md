
<!-- README.md is generated from README.Rmd. Please edit that file -->

# disk.frame <img src="inst/figures/disk.frame.png" align="right">

# Introduction

How do I manipulate tabular data that doesn’t fit into Random Access
Memory (RAM)?

Use `{disk.frame}`\!

In a nutshell, `{disk.frame}` makes use of two simple ideas

1)  split up a larger-than-RAM dataset into chunks and store each chunk
    in a separate file inside a folder and
2)  provide a convenient API to manipulate these chunks

`{disk.frame}` performs a similar role to distributed systems such as
Apache Spark, Python’s Dask, and Julia’s JuliaDB.jl for *medium data*
which are datasets that are too large for RAM but not quite large enough
to qualify as *big data*.

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
    `{disk.frame}`](https://diskframe.com/articles/intro-disk-frame.html)
    which replicates the `sparklyr` vignette for manipulating the
    `nycflights13` flights data.
  - [Ingesting data into
    `{disk.frame}`](https://diskframe.com/articles/ingesting-data.html)
    which lists some commons way of creating disk.frames
  - [`{disk.frame}` can be more
    epic\!](https://diskframe.com/articles/more-epic.html) shows some
    ways of loading large CSVs and the importance of `srckeep`
  - [Group-by](https://diskframe.com/articles/group-by.html) the various
    types of group-bys
  - [Custom one-stage group-by
    functions](https://diskframe.com/articles/custom-group-by.html) how
    to define custom one-stage group-by functions
  - [Fitting GLMs (including logistic
    regression)](https://diskframe.com/articles/glm.html) introduces the
    `dfglm` function for fitting generalized linear models
  - [Using data.table syntax with
    disk.frame](https://diskframe.com/articles/data-table-syntax.html)
  - [disk.frame concepts](https://diskframe.com/articles/concepts.html)
  - [Benchmark 1: disk.frame vs Dask vs
    JuliaDB](https://diskframe.com/articles/vs-dask-juliadb.html)

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
(e.g. the `cmap(a.disk.frame, fn, lazy = F)` function which applies the
function `fn` to each chunk of `a.disk.frame` in parallel). So that
`{disk.frame}` can be manipulated in a similar fashion to in-memory
`data.frame`s.

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

## Example: dplyr verbs

### dplyr verbs

{disk.frame} aims to support as many dplyr verbs as possible. For
example

``` r
flights.df %>% 
  filter(year == 2013) %>% 
  mutate(origin_dest = paste0(origin, dest)) %>% 
  head(2)
#>   year month day dep_time sched_dep_time dep_delay arr_time sched_arr_time
#> 1 2013     1   1      517            515         2      830            819
#> 2 2013     1   1      533            529         4      850            830
#>   arr_delay carrier flight tailnum origin dest air_time distance hour minute
#> 1        11      UA   1545  N14228    EWR  IAH      227     1400    5     15
#> 2        20      UA   1714  N24211    LGA  IAH      227     1416    5     29
#>             time_hour origin_dest
#> 1 2013-01-01 05:00:00      EWRIAH
#> 2 2013-01-01 05:00:00      LGAIAH
```

### Group-by

Starting from `{disk.frame}` v0.3.0, there is `group_by` support for a
limited set of functions. For example:

``` r
result_from_disk.frame = iris %>% 
  as.disk.frame %>% 
  group_by(Species) %>% 
  summarize(
    mean(Petal.Length), 
    sumx = sum(Petal.Length/Sepal.Width), 
    sd(Sepal.Width/ Petal.Length), 
    var(Sepal.Width/ Sepal.Width), 
    l = length(Sepal.Width/ Sepal.Width + 2),
    max(Sepal.Width), 
    min(Sepal.Width), 
    median(Sepal.Width)
    ) %>% 
  collect
```

The results should be exactly the same as if applying the same group-by
operations on a data.frame. If not, please [report a
bug](https://github.com/xiaodaigh/disk.frame/issues).

#### List of supported group-by functions

If a function you like is missing, please make a feature request
[here](https://github.com/xiaodaigh/disk.frame/issues). It is a
limitation that function that depend on the order a column can only be
obtained using estimated methods.

| Function     | Exact/Estimate | Notes                                      |
| ------------ | -------------- | ------------------------------------------ |
| `min`        | Exact          |                                            |
| `max`        | Exact          |                                            |
| `mean`       | Exact          |                                            |
| `sum`        | Exact          |                                            |
| `length`     | Exact          |                                            |
| `n`          | Exact          |                                            |
| `n_distinct` | Exact          |                                            |
| `sd`         | Exact          |                                            |
| `var`        | Exact          | `var(x)` only `cor, cov` support *planned* |
| `any`        | Exact          |                                            |
| `all`        | Exact          |                                            |
| `median`     | Estimate       |                                            |
| `quantile`   | Estimate       | One quantile only                          |
| `IQR`        | Estimate       |                                            |

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

suppressWarnings(
  grp_by_stage1 <- 
    flights.df[
      keep = c("month", "distance"), # this analysis only required "month" and "dist" so only load those
      month <= 6, 
      .(sum_dist = sum(distance)), 
      .(qtr = ifelse(month <= 3, "Q1", "Q2"))
      ]
)

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

## Basic info

To find out where the disk.frame is stored on disk:

``` r
# where is the disk.frame stored
attr(flights.df, "path")
#> [1] "C:\\Users\\RTX2080\\AppData\\Local\\Temp\\RtmpsnJlFJ\\file3d3ce978e3.df"
```

A number of data.frame functions are implemented for disk.frame

``` r
# get first few rows
head(flights.df, 1)
#>    year month day dep_time sched_dep_time dep_delay arr_time sched_arr_time
#> 1: 2013     1   1      517            515         2      830            819
#>    arr_delay carrier flight tailnum origin dest air_time distance hour minute
#> 1:        11      UA   1545  N14228    EWR  IAH      227     1400    5     15
#>              time_hour
#> 1: 2013-01-01 05:00:00
```

``` r
# get last few rows
tail(flights.df, 1)
#>    year month day dep_time sched_dep_time dep_delay arr_time sched_arr_time
#> 1: 2013     9  30       NA            840        NA       NA           1020
#>    arr_delay carrier flight tailnum origin dest air_time distance hour minute
#> 1:        NA      MQ   3531  N839MQ    LGA  RDU       NA      431    8     40
#>              time_hour
#> 1: 2013-09-30 08:00:00
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

| Title                                                                                                                                 | Language | Author          | Date       | Description                                                                                        |
| ------------------------------------------------------------------------------------------------------------------------------------- | -------- | --------------- | ---------- | -------------------------------------------------------------------------------------------------- |
| [25 days of disk.frame](https://twitter.com/evalparse/status/1200963268270886912)                                                     | English  | ZJ              | 2019-12-01 | 25 tweets about `{disk.frame}`                                                                     |
| <https://www.researchgate.net/post/What_is_the_Maximum_size_of_data_that_is_supported_by_R-datamining>                                | English  | Knut Jägersberg | 2019-11-11 | Great answer on using disk.frame                                                                   |
| [`{disk.frame}` is epic](https://www.brodrigues.co/blog/2019-09-03-disk_frame/)                                                       | English  | Bruno Rodriguez | 2019-09-03 | It’s about loading a 30G file into `{disk.frame}`                                                  |
| [My top 10 R packages for data analytics](https://www.actuaries.digital/2019/09/26/my-top-10-r-packages-for-data-analytics/)          | English  | Jacky Poon      | 2019-09-03 | `{disk.frame}` was number 3                                                                        |
| [useR\! 2019 presentation video](https://www.youtube.com/watch?v=3XMTyi_H4q4)                                                         | English  | Dai ZJ          | 2019-08-03 |                                                                                                    |
| [useR\! 2019 presentation slides](https://www.beautiful.ai/player/-LphQ0YaJwRektb8nZoY)                                               | English  | Dai ZJ          | 2019-08-03 |                                                                                                    |
| [Split-apply-combine for Maximum Likelihood Estimation of a linear model](https://www.brodrigues.co/blog/2019-10-05-parallel_maxlik/) | English  | Bruno Rodriguez | 2019-10-06 | `{disk.frame}` used in helping to create a maximum likelihood estimation program for linear models |
| [Emma goes to useR\! 2019](https://emmavestesson.netlify.com/2019/07/user2019/)                                                       | English  | Emma Vestesson  | 2019-07-16 | The first mention of `{disk.frame}` in a blog post                                                 |
| [深入对比数据科学工具箱：Python3 和 R 之争(2020版)](https://segmentfault.com/a/1190000021653567)                                                      | Chinese  | Harry Zhu       | 2020-02-16 | Mentions disk.frame                                                                                |

### Interested in learning `{disk.frame}` in a structured course?

Please register your interest at:

<https://leanpub.com/c/taminglarger-than-ramwithdiskframe>

## Open Collective

If you like `{disk.frame}` and want to speed up its development or
perhaps you have a feature request? Please consider sponsoring
`{disk.frame}` on Open Collective

### Backers

Thank you to all our backers\!

<a href="https://opencollective.com/diskframe#backers" target="_blank"><img src="https://opencollective.com/diskframe/backers.svg?width=890"></a>

### Sponsor and back `{disk.frame}`

Support `{disk.frame}` development by becoming a sponsor. Your logo will
show up here with a link to your website.

<a href="https://opencollective.com/diskframe#sponsors" target="_blank"><img src="https://opencollective.com/diskframe/sponsors.svg?width=890"></a>

## Contact me for consulting

**Do you need help with machine learning and data science in R, Python,
or Julia?** I am available for Machine Learning/Data
Science/R/Python/Julia consulting\! [Email
me](mailto:dzj@analytixware.com)

## Non-financial ways to contribute

Do you wish to give back the open-source community in non-financial
ways? Here are some ways you can contribute

  - Write a blogpost about your `{disk.frame}`. I would love to learn
    more about how `{disk.frame}` has helped you
  - Tweet or post on social media (e.g LinkedIn) about `{disk.frame}` to
    help promote it
  - Bring attention to typos and grammatical errors by correcting and
    making a PR. Or simply by [raising an issue
    here](https://github.com/xiaodaigh/disk.frame/issues)
  - Star the [`{disk.frame}` Github
    repo](https://github.com/xiaodaigh/disk.frame)
  - Star any repo that `{disk.frame}` depends on
    e.g. [`{fst}`](https://github.com/fstpackage/fst) and
    [`{future}`](https://github.com/HenrikBengtsson/future)

## Related Repos

<https://github.com/xiaodaigh/disk.frame-fannie-mae-example>
<https://github.com/xiaodaigh/disk.frame-vs>
<https://github.com/xiaodaigh/disk.frame-fannie-mae-example>
<https://github.com/xiaodaigh/disk.frame.ml>
<https://github.com/xiaodaigh/courses-larger-than-ram-data-manipulation-with-disk-frame>

## Download Counts & Build Status

[![](https://cranlogs.r-pkg.org/badges/disk.frame)](https://cran.r-project.org/package=disk.frame)
[![](http://cranlogs.r-pkg.org/badges/grand-total/disk.frame)](https://cran.r-project.org/package=disk.frame)
[![Travis build
status](https://travis-ci.org/xiaodaigh/disk.frame.svg?branch=master)](https://travis-ci.org/xiaodaigh/disk.frame)
[![AppVeyor build
status](https://ci.appveyor.com/api/projects/status/github/xiaodaigh/disk.frame?branch=master&svg=true)](https://ci.appveyor.com/project/xiaodaigh/disk.frame)

## Live Stream of `{disk.frame}` development

  - <https://www.youtube.com/playlist?list=PL3DVdT3kym4fIU5CO-pxKtWhdjMVn4XGe>
