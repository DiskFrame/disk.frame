context("test-setup")

test_that("testing sas_to_disk.frame", {
  setup_disk.frame(workers = 2)
  a = future::nbrOfWorkers()
  expect_equal(a, 2)
})