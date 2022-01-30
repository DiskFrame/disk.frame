context("test-util")

test_that("testing evalparseglue", {
  x = 2
  y = 3
  expect_equal(evalparseglue("{x}+{y}"), 5)
})

