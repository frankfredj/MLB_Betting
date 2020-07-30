################################################
################ INITIALISATION ################
################################################

n = 25
path = "D:/MLB"

library("FNN")
library("missRanger")
library("ggplot2")
library("e1071")
library("caret")
library("glmnet")
library("viridis")
library("pracma")
library("reshape2")
library("doParallel")
library("cowplot")

################################################
################################################


################################################
################ Functions #####################
################################################

#Given an index (a1 ... ak), produces an index with (a1_home, a1_away, ... , ak_home, ak_away)
symetrical_index <- function(index, X){

	m <- ncol(X) / 2

	u <- which(index > m)
	if(length(u) > 0){

		a <- index[u] - m
		b <- index[-u] + m		

	} else {

		a <- index
		b <- index + m	

	}

	return(sort(unique(c(a,b,index))))

}



#Plot the stacked ecdf and epdf functions of a vector
density_plots <- function(x, varname, title){

	unique_vals <- unique(sort(x))
	pdf <- ecdf(x)

	plot_frame <- as.data.frame(matrix(nrow = length(unique_vals), ncol = 3))
	colnames(plot_frame) <- c("X", "Density", "Ecdf")
	plot_frame$X <- unique_vals

	f <- approxfun(density(x))
	plot_frame$Epdf <- f(plot_frame$X)

	f <- ecdf(x)
	plot_frame$Ecdf <- f(plot_frame$X)

	plot_frame$Ecdf <- plot_frame$Ecdf / max(plot_frame$Ecdf)
	plot_frame$Epdf <- plot_frame$Epdf / max(plot_frame$Epdf)

	k <- nrow(plot_frame)

	melted_plot <- as.data.frame(matrix(nrow = 2*k, ncol = 3))
	colnames(melted_plot) <- c("X", "Y", "Legend")
	melted_plot$Y <- c(plot_frame$Epdf, plot_frame$Ecdf)
	melted_plot$X <- c(plot_frame$X, plot_frame$X)
	melted_plot$Legend <- c(rep("Epdf", k), rep("Ecdf", k))

	out <- ggplot(melted_plot, aes(x = X, y = Y, fill = Legend)) + geom_step(aes(x = X, y = Y, color = Legend), alpha = 2, size = 1.5) +
															geom_ribbon(aes(ymin = 0, ymax = Y, fill = Legend), alpha = 0.5) +
															xlab(varname) +
															ylab("Value") +
															ggtitle(title) + 
															theme(axis.title=element_text(size=14, face="bold"),
																plot.title = element_text(size=18, face="bold"))

	return(out)		

}





#Dummy function for knn outliers
knn_outliers_dummy <- function(x, nn, n_sd){

	dists <- rep(0, length(x))
	index <- order(x)
	x_sort <- x[index]

	for(i in 1:length(x)){

		index_temp <- c((i - nn - 1):(i + nn + 1))
		index_temp <- index_temp[which(index_temp >= 1 & index_temp <= length(x))]

		eucl_dist <- sort(abs(x_sort[i] - x_sort[index_temp]))[c(2:(nn+1))]
		dists[index[i]] <- mean(eucl_dist)

	}

	dists <- dists - mean(dists) / sd(dists)

	return(dists)

}



# Trim a numerical vector by recursively removing outliers until the normalized 
# distribution of knn distances given nn is comprised within [-n_sd, n_sd]
knn_outliers <- function(X, nn, n_sd, j){

	x <- as.numeric(X[, j])
	rmv <- c()
	index <- c(1:length(x))
	avg_dists <- knn_outliers_dummy(x, nn, n_sd)

	k <- 0

	old_range <- range(x)
	old_skew <- skewness(x)

	while(TRUE){
	#for(i in 1:134){
		k <- k + 1

		to_flush <- which(abs(avg_dists) >= n_sd)
		if(length(to_flush) == 0){break}

		rmv <- c(rmv, index[to_flush])

		x <- x[-to_flush]
		index <- index[-to_flush]	


		avg_dists <- knn_outliers_dummy(x, nn, n_sd)

	}

	if(length(rmv) > 0){

		print("Old range:", quote = FALSE)
		print(old_range)
		print("Old skewness:", quote = FALSE)
		print(old_skew)

		cat("\n")

		print("New range:", quote = FALSE)
		print(range(x))
		print("New skewness:", quote = FALSE)
		print(skewness(x))

		print(density_plots(x, colnames(X)[j]))

	} 

	return(rmv)

}


#Prints a correlation heatmap of selected variables
cor_heatmap <- function(frame, type, variables, title, names){

	index <- match(variables, colnames(frame))
	if(type == "cor"){

		cors <- cor(frame[, index])

	} else {

		cors <- cov(frame[, index])

	}

	plot_frame <- cbind(expand.grid(X = rownames(cors), Y = colnames(cors)), expand.grid(cors))
	colnames(plot_frame)[3] <- "Value"


	m <- ncol(cors)
	if(names){

		plt <- 	ggplot(plot_frame, aes(X, Y, fill= Value)) + geom_tile() +
  												scale_fill_viridis(discrete=FALSE)+ 
  												theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  												labs(title = title) 

	} else {


		plt <- 	ggplot(plot_frame, aes(X, Y, fill= Value)) + geom_tile() +
  												scale_fill_viridis(discrete=FALSE) +
												  theme(axis.title.x=element_blank(),
												        axis.text.x=element_blank(),
												        axis.ticks.x=element_blank(),
												        axis.title.y=element_blank(),
												        axis.text.y=element_blank(),
												        axis.ticks.y=element_blank()) +
												  labs(title = title)
	}


	return(plt)

}

################################################
################################################



#####################################################
################ Data retrieval #####################
#####################################################

#Retrieve frames, add Y binary A_Win and B_Loss variables
X <- read.csv(paste(path, "/MLB_Modeling/Regression/", n, "/X.csv", sep = ""))
X$IP_Pitch_Home <- NULL
X$IP_Pitch_Away <- NULL

m <- which(colnames(X) == "Date") - 1
data <- X[, -c(1:m)]
X <- X[, c(1:m)]

data$Win <- 1
data$Win[which(data$Score_Home < data$Score_Away)] <- 0

Y <- as.matrix(data$Win)
Y_factor <- rep("A_Win", length(Y))
Y_factor[which(Y == 0)] <- "B_Loss"
Y_factor <- as.factor(Y_factor)


#####################################################
#####################################################


#####################################################
################ Outlier removal ####################
#####################################################


#Remove near-zero variance columns
rmv <- nearZeroVar(X)
if(length(rmv) > 0){

	rmv <- symetrical_index(rmv, X)
	X <- X[, -rmv]

}

#Remove linear combination so that X is not rank-deficient
rmv <- findLinearCombos(X)$remove
if(length(rmv) > 0){

	rmv <- symetrical_index(rmv, X)
	X <- X[, -rmv]	

}



#Remove data with gross outliers, using the knn algorithm
#Compute the row-index of values which are outliers wrt to each columns
to_impute <- list()

#number of nn used for knn distances
nn <- 20
#maximum number of sds away from the mean
n_sd <- 3

pb <- txtProgressBar(min = 0, max = ncol(X), style = 3)
for(j in 1:ncol(X)){
	to_impute[[j]] <- knn_outliers(X, nn, n_sd, j)
	setTxtProgressBar(pb, j)
}

keep <- which(!unlist(lapply(to_impute, is.null)))
to_impute <- to_impute[keep]
outliers <- keep

p_to_impute <- unlist(lapply(to_impute, length)) / nrow(X)
rmv <- unique(unlist(to_impute))


if(length(rmv) > 0){

	X <- X[-rmv, ]
	data <- data[-rmv, ]
	Y <- Y[-rmv]
	Y_factor <- Y[-rmv]

}




#Remove columns with abnormal data spread using QCoD
#Plot QCoD
f <- function(x)(return(quantile(x, 0.75) + quantile(x, 0.25)))
QCoD <- abs(apply(X, 2, IQR) / apply(X, 2, f))

#Remove data with 0 qcd
rmv <- which(is.na(QCoD))
if(length(rmv) > 0){

	rmv <- symetrical_index(rmv, X)
	X <- X[, -rmv]

	QCoD <- abs(apply(X, 2, IQR) / apply(X, 2, f))

}


density_plots(QCoD, "Quantile Coefficient of Dispersion")


#####################################################
#####################################################


#####################################################
################ Data visualization #################
#####################################################

#p-values for t-tests
m <- ncol(X)/2
means <- list()
means$home <- apply(X[, c(1:m)], 2, mean)
means$away <- apply(X[, -c(1:m)], 2, mean)

vars <- list()
vars$home <- apply(X[, c(1:m)], 2, var)
vars$away <- apply(X[, -c(1:m)], 2, var)

scores <- sqrt(nrow(X)) * (means$home - means$away) / sqrt(vars$home + vars$away)
p_vals <- pnorm(scores)

print(density_plots(p_vals, "Value", "T-test p-values"))


#Plot skewness
skews <- apply(X, 2, skewness)
print(density_plots(skews, "Value",  "Skewness"))



#####################################################
#####################################################


#############################################################
################ PCA Monte Carlo Simulator ##################
#############################################################


#Use C++ w/ rcpp
library("Rcpp")
library("RcppArmadillo")
library("RcppParallel")
cpp_path <- "D:/MCMC.cpp"
sourceCpp(cpp_path )


X$Score_Home <- data$Score_Home
X$Score_Away <- data$Score_Away

X$Open_Home <- data$Open_Home
X$Open_Away <- data$Open_Away


#Obtain Cholesky matrix
L <- chol(cov(X))
L_inv <- solve(L)
L_t <- t(L)

#Obtain principal components
SortedPrComp <- as.matrix(X) %*% L_inv
for(j in 1:ncol(SortedPrComp)){SortedPrComp[, j] <- sort(SortedPrComp[, j])}

#Obtain lower and upper bounds on the columns of X
Bounds <- matrix(nrow = nrow(L), ncol = 2)
Bounds[, 1] <- apply(X, 2, min)
Bounds[, 2] <- apply(X, 2, max)


#MCMC
big_sim <- Constrained_MCMC_parallel(SortedPrComp, L_t, Bounds, nrow(X), 8)
colnames(big_sim) <- colnames(X)


################################################################################################################
################ Disparities between the artificial data and the empirical one #################################
################################################################################################################


vars <- colnames(big_sim)
cowplot::plot_grid(plotlist = list(cor_heatmap(X, "cor", vars, "Empirical Data Correlation Heatmap", FALSE), 
									cor_heatmap(big_sim, "cor", vars, "Synthetic Data Correlation Heatmap", FALSE)), 
										nrow = 1)



#p-values for t-tests
mu_diff <- apply(X, 2, mean) - apply(big_sim, 2, mean)
sds <- sqrt(2) * apply(X, 2, sd) / sqrt(nrow(X))

scores <- mu_diff / sds
p_vals <- pnorm(scores)
p_vals[which(p_vals < 0.5)] <- 1 - p_vals[which(p_vals < 0.5)]

cowplot::plot_grid(plotlist = list(density_plots(scores, "scaled mean differences", "Normalized differences in means"),
									density_plots(p_vals, "p-value", "T-test p-values")), 
									nrow = 1)



#######################################################################
################ SAVE REGRESSION FILE #################################
#######################################################################

MCMC_toolkit <- list()
MCMC_toolkit$SortedPrComp <- SortedPrComp
MCMC_toolkit$L <- L_t
MCMC_toolkit$Bounds <- Bounds
MCMC_toolkit$rcpp_path <- cpp_path

#Save the processed regression frame
path_save <- paste(path, "/MLB_Modeling/Regression/", n, "/MCMC_toolkit.rds", sep = "")
saveRDS(X, path_save)

X$Score_Home <- NULL
X$Score_Away <- NULL

X$Open_Home <- NULL
X$Open_Away <- NULL

#Save the processed regression frame
path_save <- paste(path, "/MLB_Modeling/Regression/", n, "/X_Clean.rds", sep = "")
saveRDS(X, path_save)

#Save the processed regression frame
path_save <- paste(path, "/MLB_Modeling/Regression/", n, "/Y_Clean.rds", sep = "")
saveRDS(data, path_save)