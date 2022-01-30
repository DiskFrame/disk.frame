context("test-recommend_nchunk")

test_that("testing df_ram_size", {
  expect_true(is.numeric(df_ram_size()))
  
  expect_true(!is.na(df_ram_size()))
  expect_true(!is.null(df_ram_size()))
  expect_true(!is.nan(df_ram_size()))
  expect_true(is.finite(df_ram_size()))
})
  
test_that("testing df_ram_size; guards #213", {
  # TODO tests
  expect_true(df_ram_size() >= 1)
})

