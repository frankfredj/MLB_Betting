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

	original_data <- x

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

		plot_a <- density_plots(original_data, colnames(X)[j], "Original Data")
		plot_b <- density_plots(x, colnames(X)[j], "Clean Data")

		print(cowplot::plot_grid(plotlist = list(plot_a, plot_b), nrow = 1))


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
X <- as.data.frame(readRDS(paste(path, "/MLB_Modeling/Regression/", n, "/R_regression_matrix.rds", sep = "")))
IDs <- as.numeric(rownames(X))

data <- read.csv(paste(path, "/MLB_Modeling/Scores/Clean_Data/FanGraphs_Scores.csv", sep = ""))
index <- match(IDs, data$ID)
data <- data[index, ]

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


#Remove data with gross outliers, using the knn algorithm
#Compute the row-index of values which are outliers wrt to each columns
to_impute <- list()

#number of nn used for knn distances
nn <- 20
#maximum number of sds away from the mean
n_sd <- 3.5

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


saveRDS(X, paste(path, "/MLB_Modeling/Regression/", n, "/R_regression_matrix_clean.rds", sep = ""))
saveRDS(data, paste(path, "/MLB_Modeling/Regression/", n, "/R_regression_data_clean.rds", sep = ""))




