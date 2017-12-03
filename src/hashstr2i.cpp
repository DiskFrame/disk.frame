#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
NumericVector hashstr2i(std::vector< std::string >  x, int ngrps) {
  NumericVector out(x.size());
  
  for(uint32_t i =0; i < x.size(); i++)
  {
    std::string s = x[i];
    uint32_t hash = 0;
    for(uint32_t j =0; j < s.size(); j++) {
      hash += s[j];
      hash += (hash << 10);
      hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    
    out[i] = (hash % ngrps) + 1;
  }
  
  return out;
}


// https://stackoverflow.com/questions/114085/fast-string-hashing-algorithm-with-low-collision-rates-with-32-bit-integer