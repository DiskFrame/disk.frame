context("test-recommend_nchunk")

test_that("testing df_ram_size; guards #213", {
  # TODO tests
  expect_true(df_ram_size() >= 1)
})

