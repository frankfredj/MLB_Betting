#include <RcppArmadillo.h>
#include <random>
#include <chrono>
#include <omp.h>
#include <numeric>
// [[Rcpp::depends(RcppArmadillo)]]



/////////////////////////////////// Functions and sub-functions for running MCMC ///////////////////////////////////

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





/////////////////////////////////// Functions and sub-functions for computing averages ///////////////////////////////////








// [[Rcpp::export]]
arma::mat column_means_parallel(const arma::mat& X, int ncores){

  int m = X.n_cols;
  int n = X.n_rows;

  arma::mat out(1, m);

  omp_set_num_threads(ncores);
  # pragma omp parallel for
  for(int i = 0; i < m; i++){

    out(0, i) = arma::accu(X.unsafe_col(i)) / n;

  };

  return out;

}


// [[Rcpp::export]]
arma::rowvec column_means(const arma::mat& X){

  return arma::mean(X, 0);

}


// [[Rcpp::export]]
arma::mat column_sums_parallel(const arma::mat& X, int ncores){

  int m = X.n_cols;

  arma::mat out(1, m);

  omp_set_num_threads(ncores);
  # pragma omp parallel for
  for(int i = 0; i < m; i++){

    out(0, i) = arma::accu(X.unsafe_col(i));

  };

  return out;

}


// [[Rcpp::export]]
arma::mat column_sums(const arma::mat& X){

  return arma::sum(X, 0);

}



// [[Rcpp::export]]
arma::uvec find_equal(const arma::vec& X, double val){

  return arma::find(X == val) + 1;

}


// [[Rcpp::export]]
arma::uvec player_indices(const arma::vec& name_vec, int name, const arma::vec& date_vec, int date, int ndays){
  
  bool empty = false;

  arma::uvec index1;
  arma::uvec index2;
  arma::uvec index3;    

  while(true){

    index1 = arma::find(date_vec < date);
    if(index1.n_elem == 0){

      empty = true;
      break;

    };


    index2 = arma::find(date_vec(index1) >= date - ndays);
    if(index2.n_elem == 0){

      empty = true;
      break;

    };

    index3 = arma::find(name_vec(index1(index2)) == name);  
    if(index3.n_elem == 0){

      empty = true;
      break;

    };

    break;    

  };


  arma::uvec out;

  if(!empty){

    out = index1(index2(index3)) + 1;

  } 


  return out;

}




// [[Rcpp::export]]
arma::rowvec player_row_query(const arma::mat& X, const arma::vec& name_vec, int name, const arma::vec& date_vec, int date, int ndays){

  arma::rowvec out;
  arma::uvec index = player_indices(name_vec, name, date_vec, date, ndays) - 1;
  int n = index.n_elem - 1;

  if(n >= 0){

    out = arma::mean(X.rows(index(0), index(n)), 0);

  }

  return out;

}




// [[Rcpp::export]]
arma::field<arma::rowvec> players_row_query_list(const arma::mat& X, const arma::vec& name_vec, const arma::vec& names, 
                                  const arma::vec& date_vec, int date, int ndays){

  int n = names.n_elem;
  arma::field<arma::rowvec> out(n);

  for(int i = 0; i < n; i++){

    out(i) = player_row_query(X, name_vec, names(i), date_vec, date, ndays);

  };

return out;

}


// [[Rcpp::export]]
arma::rowvec players_row_query_mean(const arma::mat& X, const arma::vec& name_vec, const arma::vec& names, 
                                  const arma::vec& date_vec, int date, int ndays){

  arma::field<arma::rowvec> query = players_row_query_list(X, name_vec, names, date_vec, date, ndays);
  arma::rowvec out;

  int n = query.n_elem;
  int m = 0;
  int non_empty_elements = 0;

  for(int i = 0; i < n; i++){

    if(query(i).n_elem > 0){

      m = query(i).n_elem;
      non_empty_elements += 1;

    };

  };

  arma::mat to_avg(non_empty_elements, m);
  int k = 0;

  if(m > 0){

    for(int i = 0; i < n; i++){

      if(query(i).n_elem > 0){

        to_avg.row(k) = query(i);
        k += 1;

      };

    };

    out = arma::mean(to_avg, 0);  

  };

  return out;

}









// [[Rcpp::export]]
arma::mat players_row_query_weight_vec(const arma::mat& X, const arma::vec& name_vec, const arma::vec& names, 
                                  const arma::vec& date_vec, int date, int ndays, int weight_column_index){

  arma::field<arma::rowvec> query = players_row_query_list(X, name_vec, names, date_vec, date, ndays);
  arma::mat out;

  weight_column_index -= 1;

  int n = query.n_elem;
  int m = 0;
  int non_empty_elements = 0;

  for(int i = 0; i < n; i++){

    if(query(i).n_elem > 0){

      m = query(i).n_elem;
      non_empty_elements += 1;

    };

  };

  arma::mat to_avg(non_empty_elements, m);
  arma::rowvec weights;

  int k = 0;

  if(m > 0){

    for(int i = 0; i < n; i++){

      if(query(i).n_elem > 0){

        to_avg.row(k) = query(i);
        k += 1;

      };

    };

    weights = to_avg.col(weight_column_index).t();
    to_avg.shed_col(weight_column_index);

    out = weights * to_avg / arma::accu(weights) ;  

  };

  return out;

}



// [[Rcpp::export]]
arma::field<arma::vec> query_roster(double ID,  const arma::vec& scores_IDs,
                                                const arma::vec& scores_dates, 
                                                const arma::vec& scores_teams_home, 
                                                const arma::vec& scores_teams_away,

                                                const arma::vec& bat_IDs,
                                                const arma::vec& bat_names, 
                                                const arma::vec& bat_teams,


                                                const arma::vec& pitch_IDs,
                                                const arma::vec& pitch_names,
                                                const arma::vec& pitch_teams,
 

                                                const arma::vec& pitch_relief_IDs,
                                                const arma::vec& pitch_relief_names,
                                                const arma::vec& pitch_relief_teams){

  arma::mat out_dummy;
  arma::field<arma::vec> out(7);

  //Obtain which team played at home
  arma::uvec scores_index = arma::find(scores_IDs == ID);

  int index_int = (int)scores_index(0);

  int team_home = scores_teams_home(index_int);
  int team_away = scores_teams_away(index_int);
  int date = scores_dates(index_int);


  //Obtain the rosters
  //BAT
  arma::uvec bat_index = arma::find(bat_IDs == ID);
  arma::uvec bat_home_index = bat_index(arma::find(bat_teams(bat_index) == team_home));
  out(0) = bat_names(bat_home_index);

  arma::uvec bat_away_index = bat_index(arma::find(bat_teams(bat_index) == team_away));
  out(3) = bat_names(bat_away_index); 

  //STARTING PITCHER
  arma::uvec pitch_index = arma::find(pitch_IDs == ID);
  arma::uvec pitch_home_index = pitch_index(arma::find(pitch_teams(pitch_index) == team_home));
  out(1) = pitch_names(pitch_home_index);

  arma::uvec pitch_away_index = pitch_index(arma::find(pitch_teams(pitch_index) == team_away));
  out(4) = pitch_names(pitch_away_index);   

  //RELIEF PITCHER
  arma::uvec pitch_relief_index = arma::find(pitch_relief_IDs == ID);
  arma::uvec pitch_relief_home_index = pitch_relief_index(arma::find(pitch_relief_teams(pitch_relief_index) == team_home));
  out(2) = pitch_relief_names(pitch_relief_home_index);  

  arma::uvec pitch_relief_away_index = pitch_relief_index(arma::find(pitch_relief_teams(pitch_relief_index) == team_away));
  out(5) = pitch_relief_names(pitch_relief_away_index); 


  arma::vec date_vec(1);
  date_vec(0) = date;
  out(6) = date_vec;

  return out;

}  



// [[Rcpp::export]]
arma::rowvec query_X_row_by_ID(double ID, int ndays, int weight_j_bat, int weight_j_pitch, int ncols,

                                                const arma::vec& scores_IDs,
                                                const arma::vec& scores_dates, 
                                                const arma::vec& scores_teams_home, 
                                                const arma::vec& scores_teams_away,

                                                const arma::mat& bat,
                                                const arma::vec& bat_IDs,
                                                const arma::vec& bat_names, 
                                                const arma::vec& bat_teams,
                                                const arma::vec& bat_dates,

                                                const arma::mat& pitch,
                                                const arma::vec& pitch_names,
                                                const arma::vec& pitch_dates, 

                                                const arma::mat& pitch_starting,
                                                const arma::vec& pitch_starting_IDs,
                                                const arma::vec& pitch_starting_names,
                                                const arma::vec& pitch_starting_teams,
                                                const arma::vec& pitch_starting_dates,                                                 

                                                const arma::mat& pitch_relief,
                                                const arma::vec& pitch_relief_IDs,
                                                const arma::vec& pitch_relief_names,
                                                const arma::vec& pitch_relief_teams,
                                                const arma::vec& pitch_relief_dates){
  //Output if missing roosters
  arma::rowvec out_null(ncols);
  out_null.fill(0);

  //Obtain roosters
  arma::field<arma::vec> roosters = query_roster(ID,  scores_IDs,
                                                scores_dates, 
                                                scores_teams_home, 
                                                scores_teams_away,

                                                bat_IDs,
                                                bat_names, 
                                                bat_teams,

                                                pitch_starting_IDs,
                                                pitch_starting_names,
                                                pitch_starting_teams,
 
                                                pitch_relief_IDs,
                                                pitch_relief_names,
                                                pitch_relief_teams);


  //Check for empty elements
  int rooster_n = roosters.n_elem;
  int n_empty = 0;
  for(int i = 0; i < rooster_n - 1; i++){

    if(roosters(i).n_elem == 0){

      n_empty += 1;
      break;

    };

  };

  //Return null value if there are missing elements
  if(n_empty >= 1){

    return out_null;

  };


  //Obtain data vectors
  arma::field<arma::rowvec> out(6);

  int date = (int) roosters(6)(0);

  int ps1, ps2, pr1, pr2;
  ps1 = (int) roosters(1)(0);
  pr1 = (int) roosters(2)(0);

  ps2 = (int) roosters(4)(0);
  pr2 = (int) roosters(5)(0);  


  for(int i = 0; i < rooster_n - 1; i++){

    out(0) = players_row_query_weight_vec(bat, bat_names, roosters(0), bat_dates, date, ndays, weight_j_bat);
    out(1) = player_row_query(pitch, pitch_names, ps1, pitch_dates, date, ndays);
    out(2) = player_row_query(pitch_relief, pitch_relief_names, pr1, pitch_relief_dates, date, ndays);

    out(3) = players_row_query_weight_vec(bat, bat_names, roosters(3), bat_dates, date, ndays, weight_j_bat);
    out(4) = player_row_query(pitch, pitch_names, ps2, pitch_dates, date, ndays);
    out(5) = player_row_query(pitch_relief, pitch_relief_names, pr2, pitch_relief_dates, date, ndays);

  };  


  //Check for empty elements
  rooster_n = out.n_elem;
  n_empty = 0;
  for(int i = 0; i < rooster_n; i++){

    if(out(i).n_elem == 0){

      n_empty += 1;
      break;

    };

  };

  //Return null value if there are missing elements
  if(n_empty >= 1){

    return out_null;

  };



  double w1, w2, t;
  w1 = (double) out(1)(weight_j_bat - 1);
  w2 = (double) out(2)(weight_j_bat - 1);
  t = w1 + w2;
  w1 /= t;
  w2 /= t;

  arma::rowvec pitch_home = w1 * out(1) + w2 * out(2);
  pitch_home.shed_col(weight_j_bat - 1);

  w1 = (double) out(4)(weight_j_bat - 1);
  w2 = (double) out(5)(weight_j_bat - 1);
  t = w1 + w2;
  w1 /= t;
  w2 /= t;

  arma::rowvec pitch_away = w1 * out(4) + w2 * out(5);
  pitch_away.shed_col(weight_j_bat - 1);

  return arma::join_horiz(out(0), pitch_home, out(3), pitch_away);

}




// [[Rcpp::export]]
arma::mat query_ID_list(arma::vec IDs, int ndays, int weight_j_bat, int weight_j_pitch, int ncols, int ncores,

                                                const arma::vec& scores_IDs,
                                                const arma::vec& scores_dates, 
                                                const arma::vec& scores_teams_home, 
                                                const arma::vec& scores_teams_away,

                                                const arma::mat& bat,
                                                const arma::vec& bat_IDs,
                                                const arma::vec& bat_names, 
                                                const arma::vec& bat_teams,
                                                const arma::vec& bat_dates,

                                                const arma::mat& pitch,
                                                const arma::vec& pitch_names,
                                                const arma::vec& pitch_dates, 

                                                const arma::mat& pitch_starting,
                                                const arma::vec& pitch_starting_IDs,
                                                const arma::vec& pitch_starting_names,
                                                const arma::vec& pitch_starting_teams,
                                                const arma::vec& pitch_starting_dates,                                                 

                                                const arma::mat& pitch_relief,
                                                const arma::vec& pitch_relief_IDs,
                                                const arma::vec& pitch_relief_names,
                                                const arma::vec& pitch_relief_teams,
                                                const arma::vec& pitch_relief_dates){

  int nrows = IDs.n_elem;
  arma::mat out(nrows, ncols);
  out.fill(0);

  omp_set_num_threads(ncores);
  # pragma omp parallel for
  for(int i = 0; i < nrows; i++){

  out.row(i) = query_X_row_by_ID((double) IDs(i), ndays, weight_j_bat, weight_j_pitch, ncols,

                                                scores_IDs,
                                                scores_dates, 
                                                scores_teams_home, 
                                                scores_teams_away,

                                                bat,
                                                bat_IDs,
                                                bat_names, 
                                                bat_teams,
                                                bat_dates,

                                                pitch,
                                                pitch_names,
                                                pitch_dates, 

                                                pitch_starting,
                                                pitch_starting_IDs,
                                                pitch_starting_names,
                                                pitch_starting_teams,
                                                pitch_starting_dates,                                                 

                                                pitch_relief,
                                                pitch_relief_IDs,
                                                pitch_relief_names,
                                                pitch_relief_teams,
                                                pitch_relief_dates); 


  }; 

  return(out);

}




