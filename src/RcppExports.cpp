// Generated by using Rcpp::compileAttributes() -> do not edit by hand
// Generator token: 10BE3573-1514-4C36-9D1C-5A225CD40393

#include <Rcpp.h>

using namespace Rcpp;

#ifdef RCPP_USE_GLOBAL_ROSTREAM
Rcpp::Rostream<true>&  Rcpp::Rcout = Rcpp::Rcpp_cout_get();
Rcpp::Rostream<false>& Rcpp::Rcerr = Rcpp::Rcpp_cerr_get();
#endif

// hashstr2i
NumericVector hashstr2i(std::vector< std::string > x, int ngrps, int prime1, int prime2, int prime3);
RcppExport SEXP _disk_frame_hashstr2i(SEXP xSEXP, SEXP ngrpsSEXP, SEXP prime1SEXP, SEXP prime2SEXP, SEXP prime3SEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< std::vector< std::string > >::type x(xSEXP);
    Rcpp::traits::input_parameter< int >::type ngrps(ngrpsSEXP);
    Rcpp::traits::input_parameter< int >::type prime1(prime1SEXP);
    Rcpp::traits::input_parameter< int >::type prime2(prime2SEXP);
    Rcpp::traits::input_parameter< int >::type prime3(prime3SEXP);
    rcpp_result_gen = Rcpp::wrap(hashstr2i(x, ngrps, prime1, prime2, prime3));
    return rcpp_result_gen;
END_RCPP
}

static const R_CallMethodDef CallEntries[] = {
    {"_disk_frame_hashstr2i", (DL_FUNC) &_disk_frame_hashstr2i, 5},
    {NULL, NULL, 0}
};

RcppExport void R_init_disk_frame(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
