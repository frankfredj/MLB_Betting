#include <RcppArmadillo.h>
#include <random>
#include <chrono>
#include <omp.h>
// [[Rcpp::depends(RcppArmadillo)]]

// [[Rcpp::export]]
Rcpp::List fastLm(const arma::mat& X, const arma::colvec& y) {
    int n = X.n_rows, k = X.n_cols;
        
    arma::colvec coef = arma::solve(X, y);    // fit model y ~ X
    arma::colvec res  = y - X*coef;           // residuals

    // std.errors of coefficients
    double s2 = std::inner_product(res.begin(), res.end(), res.begin(), 0.0)/(n - k);
                                                        
    arma::colvec std_err = arma::sqrt(s2 * arma::diagvec(arma::pinv(arma::trans(X)*X)));  

    return Rcpp::List::create(Rcpp::Named("coefficients") = coef,
                              Rcpp::Named("stderr")       = std_err,
                              Rcpp::Named("df.residual")  = n - k);
}




// [[Rcpp::export]]
double uniform_var(){

  //Set random seed according to time
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  static std::default_random_engine generator(seed);

  std::uniform_real_distribution<> runif(0, 1);

  return runif(generator);

}



// [[Rcpp::export]]
double Ordered_Sampler(const arma::vec& x, double uniform_variable){

  //Output

  //Variables
  int i_low;
  int i_high;

  double i;

  int n = x.n_elem - 1;

  //Draw an interpolation from the ordered vector
  i = n*uniform_variable;

  i_low = std::floor(i);
  i_high = std::ceil(i);

  return (i - i_low) * x(i_high) + (i_high - i) * x(i_low);


}


// [[Rcpp::export]]
arma::vec Find_Uniform_Bounds(const arma::vec& x, double lower_bound, double upper_bound){
  
  //Find quantiles of lower and upper bounds

  //Output
  arma::vec out(2);

  double n = x.n_elem;
  int dummy = 0;

  //lb
  for(int i = 0; i < n; i++){

    if(x(i) >= lower_bound){break;}
    dummy += 1;


  };

  out(0) = dummy/(n-1);


  //ub
  dummy = n-1;
  for(int i = n-1; i >= 0; i--){

    if(x(i) <= upper_bound){break;}
    dummy -= 1;


  };

  out(1) = dummy/(n-1);

  return out;

}


// [[Rcpp::export]]
double Constrained_Sample(const arma::vec x, double lb, double ub){

  arma::vec bounds = Find_Uniform_Bounds(x, lb, ub);

  double u = uniform_var();

  return Ordered_Sampler(x, bounds(0) + u*(bounds(1) - bounds(0)));

}


// [[Rcpp::export]]
arma::vec Constrained_Sample2(const arma::vec x, arma::vec& lb, arma::vec& ub){

  arma::vec bounds1 = Find_Uniform_Bounds(x, lb(0), ub(0));
  arma::vec bounds2 = Find_Uniform_Bounds(x, lb(1), ub(1));

  double u = uniform_var();

  arma::vec out(2);

  out(0) = Ordered_Sampler(x, bounds1(0) + u*(bounds1(1) - bounds1(0)));  
  out(1) = Ordered_Sampler(x, bounds2(0) + (1 - u)*(bounds2(1) - bounds2(0)));  

  return(out);

}




// [[Rcpp::export]]
arma::mat Constrained_MCMC(const arma::mat& SortedPrComp, const arma::mat& L, const arma::mat& Bounds){

  //Dimentions
  int m = SortedPrComp.n_cols;

  //Output
  arma::vec out(m);

  //Bounds
  double lb;
  double ub;

  //dummy
  double a;

  //Fill the output
  for(int i = 0; i < m; i++){

    //Fill bound vectors
    lb = Bounds(i,0) / L(i,i);
    ub = Bounds(i,1) / L(i,i);

    //Monte Carlo updates on bounds
    for(int j = 0; j < i; j++){

      a = L(i,j)*out(j);

      lb -= a;
      ub -= a;

    };

    lb /= L(i,i);
    ub /= L(i,i);

    //Simulate 2 principal components 

    out(i) = Constrained_Sample(SortedPrComp.unsafe_col(i), lb, ub);

    };

  //Return the simulated PrComps
  return out;


}


// [[Rcpp::export]]
arma::mat Constrained_MCMC_2(const arma::mat& SortedPrComp, const arma::mat& L, const arma::mat& Bounds){

  //Dimentions
  int m = SortedPrComp.n_cols;

  //Output
  arma::mat out(2, m);

  //Bounds
  arma::vec simulated_p_comps(2);
  arma::vec lb(2);
  arma::vec ub(2);

  arma::vec bounds1(2);
  arma::vec bounds2(2);

  //Dummy variables
  double a;
  double b;
  int i = 0;

  while(i < m){

    //Fill bound vectors
    lb.fill(Bounds(i,0));
    ub.fill(Bounds(i,1));

    //Monte Carlo updates on bounds
    for(int j = 0; j < i; j++){

      a = L(i,j)*out(0,j);
      b = L(i,j)*out(1,j);

      lb(0) -= a;
      lb(1) -= b;

      ub(0) -= a;
      ub(1) -= b;

    };

    lb /= L(i,i);
    ub /= L(i,i);

    //Simulate 2 principal components 

    bounds1 = Find_Uniform_Bounds(SortedPrComp.unsafe_col(i), lb(0), ub(0));
    bounds2 = Find_Uniform_Bounds(SortedPrComp.unsafe_col(i), lb(1), ub(1));

    //Start the simulation over again if constrained sampling fails
    if(bounds1(0) >= bounds1(1) || bounds2(0) >= bounds2(1)){

      i = 0;
      continue;

    };

    double u = uniform_var();

    out(0,i) = Ordered_Sampler(SortedPrComp.unsafe_col(i), bounds1(0) + u*(bounds1(1) - bounds1(0)));  
    out(1,i) = Ordered_Sampler(SortedPrComp.unsafe_col(i), bounds2(0) + (1 - u)*(bounds2(1) - bounds2(0)));  


    i++ ;

  };

  //Return the simulated PrComps
  return out;

}




// [[Rcpp::export]]
arma::mat Constrained_MCMC_parallel(const arma::mat& SortedPrComp, const arma::mat& L, const arma::mat& Bounds, int nsims, int ncores){

  int n = (int) nsims/2;
  int m = SortedPrComp.n_cols;

  arma::mat out(2*n, m);

  omp_set_num_threads(ncores);
  # pragma omp parallel for
  for(int i = 0; i < n; i++){

    out.rows(2*i, 2*i + 1) = Constrained_MCMC_2(SortedPrComp, L, Bounds);

  };

  return out * L.t();

}
