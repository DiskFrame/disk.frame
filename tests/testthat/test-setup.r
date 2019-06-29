context("test-setup")

test_that("testing sas_to_disk.frame", {
  setup_disk.frame(workers = 4)
  a = getOption("disk.frame.nworkers")
  expect_equal(a, 4)
})